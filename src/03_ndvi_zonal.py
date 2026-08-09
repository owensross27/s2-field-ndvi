"""Selected scenes -> tiled raster read -> NDVI UDF -> RS_ZonalStatsAll -> field_ndvi.

The pattern (verified against Sedona 1.9.1 source, see docs/build-plan.md):
- format("raster") with 256px tiles (SCL at 128px so the 20m grid lands on the
  same (x, y) tile index), streamed via s3a range reads — nothing hits disk.
- RS_ReprojectMatch(scl, red, NearestNeighbor) onto the 10m grid, then ONE
  Python raster UDF for mask + offset + NDVI + nodata.
- Fields transformed per UTM zone (explicit ST_SetSRID), broadcast;
  RS_Intersects as candidate filter; RS_ZonalStatsAll exactly once per pair.
- Incremental: (date, mgrs_tile) partitions already present are skipped.

ponytail: total_px is estimated from polygon area / 100 m^2 per 10m pixel
instead of a second (excludeNoData=false) zonal pass — halves the cost; upgrade
to an exact count only if valid_frac ever needs pixel-perfect accuracy.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import ICEBERG, QUALITY, RASTER, scope
from session import assert_versions, get_sedona

import pandas as pd
from pyspark.sql import functions as F

CAT = ICEBERG["catalog"]
def load_band(sedona, hrefs_by_scene: list[tuple[str, str]], tile_px: int, colname: str):
    """One load per scene, tagged with a literal scene_id — the reader's `name`
    column is just the basename (B04.tif), so the path can't be parsed back."""
    parts = []
    for scene_id, href in hrefs_by_scene:
        df = (sedona.read.format("raster")
              .option("tileWidth", str(tile_px)).option("tileHeight", str(tile_px))
              .load(href))
        parts.append(df.select(F.lit(scene_id).alias("scene_id"), "x", "y",
                               F.col("rast").alias(colname)))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def transformed_fields(sedona, epsgs: list[int]):
    parts = []
    for epsg in epsgs:
        parts.append(sedona.sql(f"""
            SELECT field_id, area_m2, {epsg} AS f_epsg,
                   ST_SetSRID(ST_Transform(ST_SetSRID(ST_GeomFromWKB(geom_buf_5070_wkb), 5070),
                              'EPSG:5070', 'EPSG:{epsg}'), {epsg}) AS geom
            FROM {CAT}.crop.fields
            WHERE utm_zone = {epsg - 32600}
        """))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def tile_assignment(scl, meta, fields):
    """(scene_id, field_id, x, y) by pure grid arithmetic — no spatial join.

    The reader tiles every scene on a regular grid, so tile (x, y) covers a
    fixed world square: ul_x = x0 + x*stride, with the SCL grid sharing the
    10m bands' tile indices by construction (config.yml). A field's covering
    tile range is therefore floor arithmetic on its bbox. This replaces two
    RS_Intersects broadcast joins whose SpatialIndex build re-collected the
    full field set through one serial task per scene (~838MB at mvp scope —
    the run-6 wall; docs/spark-notes.md). Grid params come from SCL because it
    is the cheap band (~1.4MB/scene vs ~400MB for the 10m pair).

    Assignment is a bbox SUPERSET of true intersections; the s.count > 0
    filter downstream drops the extras — the same refinement the
    RS_Intersects path already relied on for boundary-touch pairs.
    """
    px = RASTER["scl_tile_px"]
    grids = (scl.groupBy("scene_id").agg(
        # ScaleY is negative (north-up): uly(y) = y0 - y*stride, so both
        # expressions below recover the same per-scene constants from any tile.
        F.min(F.expr(f"RS_UpperLeftX(scl) - x * RS_ScaleX(scl) * {px}")).alias("x0"),
        F.max(F.expr(f"RS_UpperLeftY(scl) - y * RS_ScaleY(scl) * {px}")).alias("y0"),
        F.max(F.expr(f"RS_ScaleX(scl) * {px}")).alias("stride"),
        F.max("x").alias("x_hi"), F.max("y").alias("y_hi")))
    return (fields
            .join(F.broadcast(grids.join(meta.select("scene_id", "epsg"), "scene_id")),
                  F.col("f_epsg") == F.col("epsg"))
            .withColumn("ix0", F.floor((F.expr("ST_XMin(geom)") - F.col("x0")) / F.col("stride")).cast("int"))
            .withColumn("ix1", F.floor((F.expr("ST_XMax(geom)") - F.col("x0")) / F.col("stride")).cast("int"))
            .withColumn("iy0", F.floor((F.col("y0") - F.expr("ST_YMax(geom)")) / F.col("stride")).cast("int"))
            .withColumn("iy1", F.floor((F.col("y0") - F.expr("ST_YMin(geom)")) / F.col("stride")).cast("int"))
            .filter((F.col("ix1") >= 0) & (F.col("ix0") <= F.col("x_hi"))
                    & (F.col("iy1") >= 0) & (F.col("iy0") <= F.col("y_hi")))
            .withColumn("x", F.explode(F.sequence(F.greatest(F.col("ix0"), F.lit(0)),
                                                  F.least(F.col("ix1"), F.col("x_hi")))))
            .withColumn("y", F.explode(F.sequence(F.greatest(F.col("iy0"), F.lit(0)),
                                                  F.least(F.col("iy1"), F.col("y_hi")))))
            .select("scene_id", "field_id",
                    F.col("x").cast("int").alias("x"), F.col("y").cast("int").alias("y")))


def process_batch(sedona, batch: pd.DataFrame, fields=None) -> int:
    """`batch` is a slice of the todo pandas frame — one scene under per_scene,
    the whole todo set otherwise. Partitions are (date, mgrs_tile) = exactly
    one scene, so a single-scene batch is a clean restart unit: nothing here
    depends on state from a prior call, so a failed iteration can just be
    re-run without cleanup. Pass a pre-built, cached `fields` to avoid
    rebuilding the field broadcast once per scene under per_scene.
    """
    from ndvi_udf import ndvi_masked

    red = load_band(sedona, list(zip(batch.scene_id, batch.red_href)), RASTER["tile_px"], "red")
    nir = load_band(sedona, list(zip(batch.scene_id, batch.nir_href)), RASTER["tile_px"], "nir")
    scl = load_band(sedona, list(zip(batch.scene_id, batch.scl_href)), RASTER["scl_tile_px"], "scl")

    scl_masked = None
    if RASTER.get("scl_tile_skip", False):
        # SCL is ~1.4MB/scene vs ~400MB for the band pair: cheap to fully decode
        # and classify before paying for NDVI map algebra + zonal on tiles that
        # are pure nodata. This is the SCL-class half of the jiffle mask below
        # (the d == 0 zero-denominator case is per-pixel NDVI arithmetic, not
        # tile classification), so the pre-pass is conservative by construction.
        # Only fully-masked tiles (masked_frac == 1.0) are dropped — that is
        # the correctness invariant: those pixels are nodata post-mask either
        # way, and total_px comes from polygon area, not pixel count, so
        # dropping them changes nothing about the output.
        # ponytail: the reader has no spatial/keep-list pushdown, so a dropped
        # tile may still get decoded at scan time — this only skips NDVI +
        # zonal on it; upgrade to reader-side pushdown if the SCL decode
        # itself ever becomes the bottleneck.
        bad = " || ".join(f"(m == {c})" for c in QUALITY["scl_mask_classes"])
        mask_script = f"m = rast[0]; out = con(({bad}), 1.0, 0.0);"
        scl_masked = scl.withColumn(
            "masked_frac",
            F.expr(f"RS_SummaryStatsAll(RS_MapAlgebra(scl, 'D', '{mask_script}')).mean"),
        ).select("scene_id", "x", "y", "masked_frac").cache()
        # coalesce so a NULL masked_frac (not reproducible with today's data,
        # but not provably impossible either) can't fall through both branches:
        # it must land in either the dropped count or keep, never neither.
        frac = F.coalesce(F.col("masked_frac"), F.lit(0.0))
        total, fully_masked = scl_masked.agg(
            F.count("*"), F.sum((frac >= 1.0).cast("int"))
        ).first()
        keep = scl_masked.filter(frac < 1.0).select("scene_id", "x", "y")
        print(f"scl_tile_skip: dropped {fully_masked} of {total} scl tiles (fully masked)")
        red = red.join(F.broadcast(keep), ["scene_id", "x", "y"], "left_semi")
        nir = nir.join(F.broadcast(keep), ["scene_id", "x", "y"], "left_semi")

    meta = sedona.createDataFrame(batch.drop(columns=["red_href", "nir_href", "scl_href"]))
    if fields is None:
        fields = transformed_fields(sedona, sorted({int(e) for e in batch.epsg}))
    # No envelope pre-filter and no geometry broadcast anywhere below: the
    # assignment enforces the scene footprint AND f_epsg = epsg by arithmetic.
    assign = tile_assignment(scl, meta, fields)

    stacked = (red.join(nir, ["scene_id", "x", "y"]).join(scl, ["scene_id", "x", "y"])
               .join(F.broadcast(meta), "scene_id")
               # pushdown: only tiles with at least one assigned field pay for
               # NDVI. The key set is narrow ints — a few MB broadcast even
               # statewide, vs the old broadcast of full field geometries.
               .join(F.broadcast(assign.select("scene_id", "x", "y").distinct()),
                     ["scene_id", "x", "y"], "left_semi")
               .withColumn("scl10", F.expr("RS_ReprojectMatch(scl, red, 'NearestNeighbor')")))

    # Band values arrive ALREADY as surface reflectance: the GeoTools read path
    # applies the COG's internal scale/offset tags (verified empirically — red
    # tile mean 0.058, range 0.005-0.504). The manifest's scale/offset columns
    # are provenance + a DQ uniformity check, NOT pipeline inputs; applying them
    # again produces NDVI ~= -0.0002 everywhere (caught by the Block-1 gate).
    if RASTER.get("ndvi_engine", "jiffle") == "jiffle":
        bad = " || ".join(f"(m == {c})" for c in QUALITY["scl_mask_classes"])
        # rr/nn < 0: Sentinel-2 L2A's BOA offset yields slightly negative
        # reflectance in dark/shadow pixels; NDVI is only bounded in [-1, 1]
        # for non-negative bands, so those pixels are masked as non-physical
        # (run 8 measured the leak: 19 of 1.94M field-dates out of range).
        script = (f"m = rast[2]; rr = rast[0]; nn = rast[1]; "
                  f"d = nn + rr; out = con(({bad}) || (d <= 0) || (rr < 0) || (nn < 0), "
                  f"-9999.0, (nn - rr) / d);")
        tiles = (stacked
                 .withColumn("stack3", F.expr(
                     "RS_AddBand(RS_AddBand(red, nir), scl10)"))
                 .withColumn("ndvi", F.expr(f"RS_MapAlgebra(stack3, 'D', '{script}', -9999.0d)")))
    else:
        tiles = stacked.withColumn("ndvi", ndvi_masked("red", "nir", "scl10"))
    tiles = tiles.select("scene_id", "mgrs_tile", "date", "dekad", "season", "epsg", "x", "y", "ndvi")

    # (tile, field) pairs by equi-join; geometries arrive once via a parallel
    # hash join on field_id instead of a per-scene driver broadcast + R-tree.
    joined = (tiles.join(F.broadcast(assign), ["scene_id", "x", "y"])
              .join(fields.select("field_id", "area_m2", "geom"), "field_id")
              .withColumn("s", F.expr("RS_ZonalStatsAll(ndvi, geom, 1)"))
              .filter(F.col("s").isNotNull() & (F.col("s.count") > 0)))

    result = (joined.groupBy("field_id", "date", "dekad", "season", "mgrs_tile", "epsg")
              .agg(F.first("scene_id").alias("scene_id"),
                   (F.sum("s.sum") / F.sum("s.count")).alias("mean_ndvi"),
                   F.sum("s.count").cast("int").alias("valid_px"),
                   F.first("area_m2").alias("area_m2"))
              .withColumn("utm_zone", (F.col("epsg") - 32600).cast("int"))
              .withColumn("total_px", F.ceil(F.col("area_m2") / 100).cast("int"))
              .withColumn("valid_frac",
                          F.least(F.lit(1.0), F.col("valid_px") / F.col("total_px")))
              .drop("area_m2", "epsg"))

    writer = result.writeTo(f"{CAT}.crop.field_ndvi").partitionedBy("date", "mgrs_tile")
    if sedona.catalog.tableExists(f"{CAT}.crop.field_ndvi"):
        writer.append()   # never swallow a failed append as "table missing"
    else:
        writer.create()

    n = result.count()
    if scl_masked is not None:
        scl_masked.unpersist()
    return n


def main() -> None:
    sc = scope()
    sedona = get_sedona("03_ndvi_zonal")
    assert_versions(sedona)
    # re-imported (harmlessly, via sys.modules cache) inside process_batch, but
    # done here too, before the todo.empty return, so a broken ndvi_udf/numpy
    # env fails at startup like it did pre-refactor — not only once there's work
    from ndvi_udf import ndvi_masked  # noqa: F401

    scenes = (sedona.table(f"{CAT}.crop.scenes")
              .filter(F.col("selected") & F.col("mgrs_tile").isin(sc["tiles"]))
              .select("scene_id", "mgrs_tile", "date", "dekad", "season", "epsg",
                      "red_href", "nir_href", "scl_href",
                      "red_scale", "red_offset", "nir_scale", "nir_offset")
              .toPandas())
    try:
        done = {(r.date, r.mgrs_tile) for r in
                sedona.table(f"{CAT}.crop.field_ndvi")
                .select("date", "mgrs_tile").distinct().collect()}
    except Exception:
        done = set()
    todo = scenes[~scenes.apply(lambda r: (r["date"], r["mgrs_tile"]) in done, axis=1)]
    print(f"scenes: {len(scenes)} selected, {len(done)} partitions done, {len(todo)} to process")
    if todo.empty:
        return

    t0 = time.time()
    if RASTER.get("per_scene", False):
        # one scene per batch: caps the red-nir-scl join shuffle at ~400MB
        # instead of shuffling the whole todo set (run 4: ~30GB spill killed a
        # 60GB disk at 41 scenes). Each iteration appends immediately.
        # Fields built once here, not inside process_batch — the loop would
        # otherwise re-scan + re-transform + re-broadcast crop.fields on every
        # iteration for a join that only needs the epsg subset.
        fields = transformed_fields(sedona, sorted({int(e) for e in todo.epsg})).cache()
        n_total = 0
        for i in range(len(todo)):
            batch = todo.iloc[i:i + 1]
            scene_id = batch.scene_id.iloc[0]
            t_scene = time.time()
            n = process_batch(sedona, batch, fields=fields)
            dt_scene = time.time() - t_scene
            n_total += n
            print(f"per_scene: {scene_id} +{n:,} rows in {dt_scene:.0f}s")
        fields.unpersist()
    else:
        n_total = process_batch(sedona, todo)

    dt = time.time() - t0
    print(f"field_ndvi: +{n_total:,} rows, {len(todo)} scenes in {dt:.0f}s "
          f"({dt/len(todo):.0f}s/scene) — capacity-model constant")


if __name__ == "__main__":
    main()
