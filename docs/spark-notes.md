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
