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

## Run 7: the tile-grid equi-join — mvp wall broken (measured, 2026-08-09)

Raw evidence: `artifacts/run6/phase0-explain.txt.gz` (the EXPLAIN diagnostic) and
`artifacts/run7/` (per-scene log + chain markers, m7g.4xlarge us-west-2).

### Root cause, verified

Phase-0 EXPLAIN showed both RS_Intersects joins already planned as Sedona
`BroadcastIndexJoin` + `SpatialIndex RTREE` — including the LEFT SEMI, including
with no broadcast hint. The operator was never the problem. The wall was
`SpatialIndexExec`'s broadcast build: a single serial task re-collects the FULL
field set and rebuilds the R-tree for every action — twice per scene under the
per-scene loop (run 2's log: ~95s single-task stages, an 838,756,588-byte task
result, one core busy on a 32-vCPU box). Cost scales with field count and ignores
cores. Sedona-side observations worth filing upstream: the per-action serial
index rebuild, and the absence of a grid equi-join pattern for grid-aligned
rasters (Databricks Mosaic has one; GEE decomposes to the same idea internally).

### The fix

`tile_assignment()` in 03: the reader's tile grid is regular, so field-to-tile
assignment is floor arithmetic on the field bbox (grid params recovered from the
cheap SCL band). Both spatial joins became hash equi-joins on narrow int keys;
geometries move once through a parallel join on field_id. Nothing scales worse
than O(fields + tiles + true pairs). Correctness: byte-identical demo signature
(13,369 rows; count, distinct fields, sum(mean_ndvi), sum(valid_px),
sum(valid_frac) all equal), `make dq` green.

### Measured

| Scope | Before (RS_Intersects joins) | After (equi-join) |
|---|---|---|
| demo, laptop local[4] | 207 s/scene | 213 s/scene (parity — index build was never the demo cost) |
| demo, 16 vCPU x86 m6i.4xlarge | 75 s/scene (run 6b) | — |
| demo, 16 vCPU Graviton m7g.4xlarge | — | **58 s/scene**, same 13,369 rows (~23% faster at 15% lower $/hr than the x86 box) |
| mvp, per scene | **DNF — zero scenes in 100 min on 32 vCPU** | **completes: 1253 / 1027 / 1210 / 1144 / 954 s/scene** (scenes 1-5, 40-45K rows each, 16 vCPU) |

mvp is no longer superlinear-stuck; the remaining ~17-20 min/scene is genuine
work (full-tile NDVI + ~50-65K zonal calls/scene), linear in it. Five of 41
mvp partitions are computed and banked (S3 `run7/wh-mvp.tar.gz`, a valid
Iceberg warehouse — the idempotency check resumes at scene 6; restore by
extracting to a fresh container-lineage path, never by copying over an
existing warehouse).

### What finishing mvp costs now (projection, labeled as such)

~1100 s x 16 vCPU = ~4.9 vCPU-h/scene; 36 scenes remain ≈ 176 vCPU-h ≈ **~$2.50
on Graviton spot** (c7g/m7g .2xl-.4xl diversified pools, per-scene Batch array
with idempotent appends — topology + the Glue-catalog swap for concurrent
writers documented in the run-6 plan). Wall-clock ~2.5 h on 10 small workers.
The next structural lever if state/history scope needs more: the zone-raster
pass (rasterize fields once per UTM zone, per-pixel groupby — O(pixels),
field-count-free), held deliberately until measurement demands it.

## Run 8: mvp COMPLETE — fan-out, reclaim-proof, published (measured, 2026-08-09)

Raw evidence: `artifacts/run8/` (per-slice ndvi logs, merge/dq/publish logs).

**Result: `field_ndvi` = 2,457,225 rows across all mvp scene-partitions
(season-2025 + the derecho event pair), 6 tiles, GX DQ gate fully green, map
published**: 278,886 fields, FOURTEEN season dekads (May 10 - Sep 27) on the
slider + the 2020 event views, `fields.pmtiles` at 67MB (drop-densest tiling —
coalesce cannot fit 278K fields in the 500KB/tile budget at overview zooms).

Two integrity catches on the way to that number, both worth keeping:

1. **The first merge silently produced a one-slice table** (343,122 rows = slice
   a alone): every per-slice export hit a root-owned mount the container uid
   could not write, the export loop failed silently, and the manifest-level DQ
   coverage check cannot see missing `field_ndvi` tiles — so the gate stayed
   green on one-sixth of the data. Caught by recounting published features
   against expectations during the description pass; fixed by a second finisher
   that asserts all five exports exist before it is allowed to publish. Lesson
   pair: assert merge inputs, and coverage checks must look at the OUTPUT table,
   not the manifest (a field_ndvi-partition coverage expectation is the open
   follow-up).
2. **The [-1, 1] range check then failed, correctly**: 19 of 1.94M season rows
   (mostly tiny cloud-masked slivers on 15TVH, valid_px as low as 1) carried
   non-physical NDVI up to 6.99. Root cause: Sentinel-2 L2A's BOA offset yields
   slightly NEGATIVE reflectance in dark pixels, and NDVI is only bounded when
   bands are non-negative. Fix: negative-band pixels are now masked as
   non-physical in BOTH engines (jiffle script and ndvi_udf — parity preserved);
   the 19 existing rows were deleted surgically and are itemized in
   artifacts/run8/. Gate re-run: all green.

Topology that survived the night: 6 tile-sliced m7g.4xlarge spot boxes, one
warehouse per box (single writer each — no Glue swap needed for a one-off; the
Batch+Glue design remains the state-tier plan), warehouse tarball to S3 every 8
minutes, plus an on-demand finisher that resumed reclaimed slices from their
tarballs, merged all six via parquet export/append, then ran 05_dq + 04_publish.
AWS reclaimed THREE spot boxes mid-run (all .4xlarge — spot was churning that
night, not just big pools); the checkpointing absorbed every reclaim at a cost of
at most one re-run scene each. Median scene 1191s at 16 vCPU (min 263s — cloudy
event scenes where SCL masking skips tiles; max 1651s).

Two operational traps for the record: `chmod a+rX` on the data mount broke
04_publish (it WRITES data/publish — the fix run mounted the real rw data dir),
and a hidden headless-browser pane never completes MapLibre style loading (no
rAF), so paint-level map verification needs a visible pane; the published data
was verified at the file level instead (all 6 dekad props + pre/post/drop event
props present per feature).

Session economics, honest: run 8 cost ~$6.75 (spot churn + the on-demand
finisher), against ~$2 (run 6) and ~$1.70 (run 7). The equi-join is what made
any of it affordable: at run-2 rates the same 53 partitions were unreachable at
any price.

## Run 9: DiD at mvp scope — the pre-registered gate fired (measured, 2026-08-09, $0)

The event-study notebook re-ran unchanged against the merged mvp warehouse
(container, `wh-mvp-final` mounted read-only at `/opt/s2fn/warehouse`; the
notebook kernel needs `$SPARK_HOME/python/lib/pyspark.zip` and the py4j zip on
PYTHONPATH explicitly — the image ships no pip pyspark, spark-submit normally
injects it). The monotonicity assert FAILED:

| band | fields | DiD (lat-only, pre-registered) | SE |
|---|---|---|---|
| 60-79 mph | 10,790 | -0.0314 | 0.0005 |
| 80-99 mph | 1,540 | -0.0934 | 0.0022 |
| 100+ mph | 1,250 | -0.0687 | 0.0018 |

The 80-99 vs 100+ inversion is ~9 SE — real, not noise; present in raw deltas
and at stricter validity (vf >= 0.8). Forensics (artifacts/run9/):

1. **Data exonerated**: the Benton-county subset of the mvp warehouse reproduces
   the committed demo numbers EXACTLY (527/-0.0144, 142/-0.0412, 116/-0.0561, to
   4 decimals) — a full cross-validation of demo (laptop, jiffle, broadcast
   spatial join) vs mvp (Graviton fan-out, tile-grid equi-join) pipelines.
2. **The confound is longitude**: the pre-registered design matches controls on
   latitude only, on the argument that the August 2020 flash drought varied
   along the latitude gradient. Across a 3-tile longitude span that assumption
   breaks: drought was worse in western Iowa, and band 2's DiD by treated-field
   longitude tercile runs west -0.141 / mid -0.106 / east -0.040, while band 3
   (the 100+ core around Cedar Rapids) has NO western fields. Band 2 absorbs
   uncontrolled western drought and overshoots.
3. **Post-hoc lat AND lon (+/-0.15 deg both) matching restores monotonicity**:
   -0.0125 (n=5,848) / -0.0365 (n=744) / -0.0498 (n=357), consistent with the
   county-scope magnitudes. Labeled post-hoc; the pre-registered analysis is
   the one reported as the mvp result.
4. Secondary attrition: mvp_event bypasses the scene-level cloud gate, and the
   southern tiles' pre-scenes are cloudy (usable fraction 0.43 on 15TWF,
   0.49 on 15TVF vs ~0.80 on the northern tiles) — 42-54% of corn fields drop
   per band. Tile-overlap double observations (~54K of 516K event-pair rows)
   are collapsed by the notebook's MAX/MIN aggregation, same as at demo scope.

Lesson for the record: a matching assumption that holds inside one county does
not survive a 17x domain expansion, and the pre-registered monotonicity gate is
what caught it. The county headline stands as the clean identification; the mvp
run is reported as-is with the diagnosis.

## Never copy a Hadoop-catalog Iceberg warehouse

Table metadata stores the ABSOLUTE table location. A `cp` of `warehouse/` to a new
path yields tables that read from — and write into — the ORIGINAL path, silently.
Rebuild via the pipeline in the new location instead; never copy. (Discovered when
a copied dev warehouse routed a verification write back into the source tree.)
