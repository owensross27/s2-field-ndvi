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
5. Implemented behind `raster.scl_tile_skip` (default off): an SCL pre-pass drops
   fully-masked tiles before NDVI + zonal. Measured at demo scope: 375s flag-off vs
   445s flag-on — a 19% REGRESSION, because the eager SCL classify pass cost more
   than the 8 of 3,698 tiles it dropped on a nearly clear scene pair. Hypothesis:
   pays off only under real cloudiness and/or in-region reads; the EKS block decides
   whether it ever earns default-on.

Rejected: caching layers, custom readers, repartition tricks. None address the
binding constraint (bytes decoded per core).

## Flag economics measured at demo scope (opt/phase1)

Every number below is a demo-scope (1 tile, 2 scenes) measurement; treat as the
small-scope end of the curve, not the verdict.

- `per_scene` (run-4 fix): correctness-neutral (exact signature match), but
  ~1902 s/scene vs ~187 s/scene batched — roughly 10x slower when the batch is
  tiny, because each iteration pays full DAG setup and loses cross-scene scan
  parallelism. It is a memory/disk-safety lever (caps the band-join shuffle at one
  scene instead of run 4's ~30GB spill), not a speed flag. Benchmark at mvp scope
  before trusting either story.
- Per-scene fields pruning (run-5 fix): under `per_scene`, the broadcast side is
  first filtered to the scene footprint (bbox from SCL tile envelopes,
  RS_Envelope + ST_Envelope_Aggr) — the statewide field set otherwise collects as
  ONE 838MB task result, which local-mode transport cannot stream (run 5's fatal
  TaskResultLost).
- The two flags are benchmarked independently; the combination is untested.

## Container validation (measured, M4 Docker Desktop, 10GB VM)

- In-container jiffle, demo scope, spark-submit local[4]: **189 s/scene**, exact
  baseline signature — parity with the native laptop run (185-207 s/scene). The
  image is production-shaped; the laptop is now just a git client and image builder.
- python_udf under load, second measured ceiling: on macOS local mode it dies of
  kernel ENOBUFS; in the Linux container at the default 6g driver heap it dies of
  `java.lang.OutOfMemoryError: Java heap space` in the py4j "stdout writer" threads
  — the JVM-to-Python serialization of multi-MB raster rows is the constraint on
  both platforms, just hitting a different wall. Not a Docker artifact per se;
  Docker Desktop's 10GB VM only lowers the ceiling. Retry belongs on a
  big-memory cloud box (32g+ heap headroom) before writing the engine off.
  jiffle remains the default engine everywhere.
- Upstream context for that retry (researched 2026-08-08): RS_MapAlgebra/jiffle is
  **deprecated as of Sedona 1.9.1** (sedona#3214) with Python raster UDFs as the
  sanctioned path, and the official mitigation for exactly our py4j-serialization
  ceiling is shrinking the rows that cross the JVM-to-Python boundary: the 1.9.0
  raster reader's `retile`/`tileWidth`/`tileHeight` options and `RS_TileExplode`,
  with per-tile sum/count rolled up afterward for zonal stats. The cloud retry
  should benchmark python_udf both bare AND tiled (see run6-benchmark-plan.md).

## kind validation run (measured, 2026-08-08, demo scope)

- Distributed jiffle on the kind cluster (1 driver + 1 executor pod, 2g each,
  1 executor core, 7.7GiB Docker Desktop VM): **2 scenes in 1025s = 512 s/scene**,
  `field_ndvi` +13,369 rows, NDVI mean 0.794, zero nulls — validated in-container
  against the recomputed warehouse-k8s. Per-CORE this beats local[4] (512 vs
  ~756 core-s/scene); wall-clock is slower simply because one core did all the work.
  The k8s scheduler backend + separate executor pod were confirmed in the driver log
  — this run is the first true distributed execution of the pipeline.
- Trap fixed on the way (session.py): the local[4] fallback used
  `SparkConf().contains("spark.master")`, which is ALWAYS False before the first
  SparkContext exists (pyspark's SparkConf is a plain dict until the JVM gateway is
  attached), so it silently clobbered the k8s master inside the driver pod — the whole
  job ran local[4] in one 2.8Gi pod and was OOMKilled. The guard now keys on
  `PYSPARK_GATEWAY_PORT` (set by Spark's PythonRunner for anything launched via
  spark-submit, absent for bare `python` runs).

## Run 6: in-region cloud benchmark (measured, 2026-08-09, us-west-2)

Raw logs: `artifacts/run6/` (synced from the boxes before teardown; `chain.log` is
the marker summary, `arm-*.log` the full Spark output).

### Engine head-to-head — the deprecation question, answered

Demo scope (2 event scenes, tile 15TWG rasters, Benton County fields), m6i.4xlarge
on-demand (16 vCPU), `local[*]`, real 24g driver heap (verified in each log:
`MemoryStore started with capacity 14.2 GiB`), each arm run against a freshly
dropped `field_ndvi`:

| Engine | Wall-clock | s/scene | Rows | Verdict |
|---|---|---|---|---|
| jiffle (`RS_MapAlgebra`, deprecated upstream in 1.9.1) | 150s | **75** | 13,369 | current default |
| python_udf (the sanctioned forward path) | 148s | **74** | 13,369 | **parity — migrate freely** |
| python_udf + 128px tiling (`tile_px: 128`, `scl_tile_px: 64`) | 228s | **114** | 13,369 | +54%, do not use here |

All three produced **identical row counts (13,369)** — the same exact-signature
match as the laptop and kind runs, so this is correctness parity, not just
timing parity.

Three things follow:

1. **python_udf's two prior failures were memory-configuration artifacts, not an
   engine limit.** It died of kernel ENOBUFS on macOS local mode and of Java heap
   OOM in the container at 6g. Given Linux and a real 24g heap it runs clean at
   jiffle's speed. The upstream deprecation (sedona#3214) is therefore not a
   problem for this repo: switching `raster.ndvi_engine` costs nothing measured.
2. **Sedona's official big-row mitigation (retile / `RS_TileExplode`) is the wrong
   tool once heap is adequate.** Smaller tiles multiply per-call overhead —
   `config.yml` already notes "GH-2409: per-call cost is O(tile area)" — so at 128px
   the run is 54% slower for byte-identical output. Tiling is a memory lever, not a
   speed lever; reach for it only when heap is the binding constraint.
3. Python-worker activity confirms the arms really differed (grep of the logs:
   32 python-worker events on the jiffle arm vs 160 on both python_udf arms).

### The mvp scaling wall (measured failure, and the real bottleneck)

Neither mvp row completed. Both are reported here because the failures are the
finding:

| Row | Config | Result |
|---|---|---|
| 1 | mvp, `per_scene` off, `scl_tile_skip` off | **DNF** — driver heap OOM, exit 137 at 72 min |
| 2 | mvp, `per_scene` on | **DNF** — healthy and spill-free, but no single scene finished in 100 min; aborted |

The plan's ~69 s/scene in-region estimate was **modeled on download time and is
wrong by roughly two orders of magnitude at mvp scope.** The actual bottleneck is
the zonal join, and it is superlinear in *field count per scene*, not raster area:

- demo scope: full 15TWG tile rasters x **Benton County's 7,226 fields** -> 75 s/scene
- mvp scope: the same class of full-tile rasters x **the whole tile's field
  population** -> no scene completed in 100 min on 32 vCPU

Roughly 10x the fields produced >100x the time. Any future mvp/state run should
batch by **field count**, not by scene — that is the lever this run identified and
the one the capacity model should be rebuilt around. Do not quote 69 s/scene again.

### DRIVER_MEM is inert under spark-submit (cost us row 1)

`session.py` sets `spark.driver.memory` on the builder. That only reaches the JVM
when pyspark launches it — the bare-`python` Makefile path. Under `spark-submit`
the JVM is already running, the value is dropped, and the driver silently gets
Spark's **1g default**. Row 1 OOMed identically at a claimed "12g" and "64g"
because both were really 1g; only `--driver-memory` on the CLI moved it. Same
shape as the master-guard bug fixed the same day: a builder setting that is a
no-op once the JVM exists. `session.py` now warns on the inert combination.
Always confirm the real heap before trusting a run:
`grep -m1 "MemoryStore started" <log>` (capacity is ~0.6x the heap).

### Spot capacity, honestly

Two consecutive 8xlarge spot instances were reclaimed by AWS mid-run
("instance-terminated-no-capacity"), the second one killing the first engine-chain
attempt. The rerun used a small **on-demand** m6i.4xlarge and finished in 10
minutes. Lesson for a same-day portfolio run: spot is the right default for long
mvp/state work, but for a short, must-finish measurement, on-demand at
~$0.77/hr is cheaper than losing the run twice. The rerun also synced every
artifact to S3 after each arm, so an interruption could not erase evidence again.

## Never copy a Hadoop-catalog Iceberg warehouse

Table metadata stores the ABSOLUTE table location. A `cp` of `warehouse/` to a new
path yields tables that read from — and write into — the ORIGINAL path, silently.
Rebuild via the pipeline in the new location instead; never copy. (Discovered when
a copied dev warehouse routed a verification write back into the source tree.)
