# Spark engineering notes

Measured on the reference laptop (M4, 16GB, macOS, local[4], home broadband) at demo
scope: Benton County IA, 7,230 fields, 2 scenes (2020-08-04 / 2020-08-19, tile 15TWG).
Every number here is from a run in this repo, not an estimate.

## Join strategy

Field polygons broadcast to executors (631K buffered polygons is a few hundred MB,
comfortably broadcastable). Raster tiles never shuffle: the zonal join is map-side.
`RS_Intersects` is a WGS84 envelope candidate filter; `RS_ZonalStatsAll` (lenient)
resolves false positives by returning null.

## Tile sizing

`format("raster")` with tileWidth/Height 256 for the 10m bands, 128 for the 20m SCL so
both grids land on the same (x, y) tile index. 256px is not cosmetic: RS_ZonalStats cost
is O(tile area) per polygon (apache/sedona GH-2409, open), roughly 2MB transient per call
at 256px vs about 40MB at the COG-native 1024px.

## Measured optimization: semi-join pushdown before map algebra

The raster reader has no spatial filter pushdown, so a broadcast left-semi join prunes
tiles that touch no field BEFORE the NDVI map algebra runs.

| Variant | s/scene | Output |
|---|---|---|
| NDVI on all 1,849 tiles/scene | 241 | 13,369 rows |
| Semi-join first (tiles with fields only) | 207 | 13,369 rows (identical) |

14% at home bandwidth: the demo job is read-dominated, the reader still decodes every
COG tile. Expect a larger relative win in-region where reads stop dominating. Kept
because it is free at state scope and correctness-invariant.

## NDVI engine: jiffle (JVM) vs python_udf, and why

The modern path is a Python raster UDF (Sedona 1.9.1 `with_bands` + `nodata`). On macOS
local mode it is not viable under load: the JVM-to-worker socket writes carry multi-MB
raster rows and fail with kernel ENOBUFS ("No buffer space available"), killing workers
with bare EOFError. Single-row repro passes, so the logic is fine; the platform is the
limit. Default engine is therefore JVM Jiffle via RS_MapAlgebra (`ndvi_engine: jiffle`),
which also skips Python serde entirely. The UDF engine stays in the repo and is the
planned engine for the Linux/EKS runs, where the two will be benchmarked head to head.

## Reflectance scale/offset: the reader already applies it

GeoTools honors the COG's internal scale/offset tags: band values arrive as surface
reflectance (verified: red tile mean 0.058, range 0.005-0.504). Applying the STAC
manifest's scale/offset again yields NDVI of about -0.0002 everywhere, a plausible-
looking disaster caught by the Block-1 gate (corn NDVI must be in [0.6, 0.9]). The
manifest values are provenance plus a DQ uniformity check, not pipeline inputs. Note for
`make_hero.py`: rasterio does NOT auto-apply these tags; the hero script must.

## macOS local-mode limits (do not fight these on a laptop)

- `SPARK_LOCAL_IP=127.0.0.1` or the driver cannot bind (hostname resolution).
- `PYSPARK_PYTHON` pinned to the venv or workers spawn the system Python 3.13.
- local[4] with a 6g driver heap: more cores plus 10g heap memory-pressure-kills workers.
- `fs.s3a.connection.maximum 24`: unbounded pools contribute to ENOBUFS.

## Capacity model

wall_clock ~= scenes x per_scene_cost / (cores / 4). Current constant: 207s/scene at
local[4] on home broadband. Predictions to verify against cloud runs (in-region reads
will beat this constant substantially): mvp (about 90 scene-dekads) ~5h laptop; the
state tier is only sane in us-west-2.

## Scaling economics: Kubernetes is not the speed lever

The zonal workload is embarrassingly parallel over scenes: polygons broadcast, raster
tiles never shuffle, so total core-seconds are ~fixed for a given scope. Wall clock =
work / cores; cost = work x $/core-hr, roughly constant however the cores are
packaged. A 64-vCPU spot box runs the mvp ~4x faster than a 16-vCPU box for about the
same dollars. EKS adds a $0.10/hr control plane (noise) and hours of first-time
setup/ops (the real cost) while buying zero throughput a bigger single box does not
have. It stays in the plan as the Spark-on-K8s operations demonstration at state
scope, not as an economic claim. Amdahl's tail (STAC query, field load + broadcast,
driver planning) puts the practical mvp floor near 15 minutes, not 2.

Sharding without K8s: split the scope tile list across 2-4 boxes. Iceberg partitions
on (date, mgrs_tile), so concurrent appends land in disjoint partitions.

Optimization ladder, ranked by expected impact:

1. In-region reads. The 207s constant is home-broadband and the job is
   read-dominated; cloud runs re-measure it.
2. More cores (above). Linear until ~1,849 tiles/scene stops feeding tasks.
3. python_udf vs jiffle head-to-head on Linux (planned): single-pass
   mask+offset+NDVI vs the two-step jiffle path.
4. GH-2409 tile-size retune at state scale: 256 vs 512 px trades per-call zonal cost
   against task count.
5. Candidate, measure first: a second semi-join on SCL cloud fraction to skip
   decoding band tiles the mask will zero anyway. SCL is ~1.4MB per scene vs ~400MB
   for the band pair, so the gate is nearly free and the win scales with cloudiness.

Rejected: caching layers, custom readers, repartition tricks. None address the
binding constraint (bytes decoded per core).

## Cloud-run findings (runs 4-5, mvp scope, m6i.4xlarge us-west-2)

Two failed mvp runs, each with a precise root cause. Both fixes live behind flags
on the opt/phase1 branch; neither invalidates the demo-scope numbers above.

1. Band-join shuffle (run 4): the red-nir-scl join on (scene_id, x, y) SHUFFLES
   raster tiles. Invisible at demo scope (2 scenes), ~30GB of spill at 41 scenes —
   killed a 60GB root volume. Disk resize is the stopgap; per-scene processing
   (`raster.per_scene`) is the durable fix: the shuffle caps at one scene
   (~400MB) and each (date, mgrs_tile) partition commits independently.
2. Broadcast ceiling, measured (run 5): `F.broadcast(fields)` collects the
   statewide field set as ONE task result — 838,756,588 bytes serialized. The
   local-mode transport failed streaming it four times, the driver saw
   TaskResultLost, and the atomic Iceberg create() rolled back 47 minutes of
   compute. "631K buffered polygons are comfortably broadcastable" (above) is
   TRUE at demo scope and FALSE at mvp+: per-scene runs must prune fields to the
   scene tile before broadcasting.
3. Success markers must gate on exit codes: run 5's wrapper printed RUN_COMPLETE
   unconditionally after a fatal Traceback. The monitoring pattern that caught
   it: alert on the union of success AND failure signatures, never success alone.
4. In-region throughput observation (not a verified constant): 41 scenes reached
   the final write in ~47 min on 16 vCPU (~69 s/scene through the full compute
   DAG) vs 207 s/scene at local[4] on home broadband. Unverified by a completed
   write; treat as a bound until a run lands.
5. Spot had zero m6i capacity in every us-west-2 AZ that night; the run used
   on-demand at $0.768/hr. Report both prices; assume neither.
6. earth-search 502s at limit=500 are deterministic (gateway limit, ~1.8s), not
   an outage; 100-item pages are reliable. Transient 502s still occur at any
   page size — 02's retry exists for those.
