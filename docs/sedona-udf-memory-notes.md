# Sedona python_udf memory ceiling: root cause and retry recipes

Research notes behind the two measured python_udf failures (macOS ENOBUFS;
Linux-container Java heap OOM — see spark-notes.md "Container validation").
Every claim carries its source. Distilled 2026-08-08.

## Root cause: single-row pickle transport of multi-MB rasters

- `RasterType` is a Spark `UserDefinedType` (python/sedona/spark/sql/types.py).
  On PySpark 3.5, Arrow-optimized Python UDFs FALL BACK to the legacy pickle
  socket path whenever a UDT is in the schema (Spark 4.1 is the first release
  that even makes the fallback configurable). Net: every Arrow knob
  (`spark.sql.execution.arrow.*`) is INERT for this UDF.
- The wire format is Sedona's own raster serde inside pickled rows. The format
  itself is NOT new — PR #2956 / SEDONA-756 (merged 2026-05-18, milestone
  1.9.1) added a *Python-side* implementation of the existing JVM format,
  explicitly byte-compatible with `Serde.deserialize()`. What 1.9.1 is new for
  is a Python UDF returning a raster directly instead of round-tripping
  through `.tolist()` + `RS_MakeRaster`. PySpark's
  `AutoBatchedSerializer` targets 64KB batches — a ~2MB raster row is ~30x
  that, so batching is already collapsed to 1 row per batch. There is no
  config that goes smaller; there is no chunking.
- The Linux OOM is JVM heap exhausted by in-flight row objects and GeoTools
  decode scratch across concurrent tasks (local[4] = up to 4 multi-MB rows in
  flight), not the socket buffer: `spark.buffer.size` backs an OFF-heap
  DirectByteBuffer (Spark PythonRunner.scala), so it cannot cause
  `Java heap space`. `spark.python.worker.memory` governs the Python side —
  also not this failure.
- NOT a known upstream issue: no apache/sedona issue, PR, or discussion
  reports raster-UDF OOM/ENOBUFS as of 2026-08-08. The serde path is about a
  week old upstream (#2956 merged for 1.9.1; correctness bug #3213 filed and
  fixed 2026-07-29/30). Worth filing our two reproductions upstream.

## Retry recipes (in order, on a big-memory Linux box)

Recipe 1 — isolate the heap variable:

```
--master local[4] --driver-memory 32g \
--conf spark.python.worker.memory=1g
```

Recipe 2 — halve in-flight rows, cut payload 4x:

```
--master local[2] --driver-memory 48g
```

plus `raster.scl_tile_px`-style reduction of the 10m tile size 256px -> 128px
(payload scales with tile AREA, so ~4x smaller rows). If Recipe 2 still OOMs,
capture a heap dump (`-XX:+HeapDumpOnOutOfMemoryError`) and file upstream —
that would be the first evidence of a real scaling ceiling rather than an
undersized rig.

## Verdict, and the strategic asterisk

jiffle-by-default is the defensible end state TODAY — two independent,
reproducible platform failures against a week-old code path. But
RS_MapAlgebra (jiffle) is OFFICIALLY deprecated in 1.9.1, with the Python
raster UDF named as its replacement (apache/sedona docs/setup/release-notes.md,
SEDONA-756: "replacing the now-deprecated RS_MapAlgebra"). No removal date is
published. So: keep `ndvi_engine: jiffle`, run the recipes on the run-6 box to
find out whether the UDF engine clears at real heap sizes, and treat the
python_udf cutover as scheduled-but-not-urgent. This is also a strong
interview artifact: the pipeline runs the deprecated-but-proven engine while
carrying measured evidence about its named successor's current limits.
