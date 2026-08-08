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
from config import ICEBERG, RASTER, scope
from session import assert_versions, get_sedona

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


def main() -> None:
    sc = scope()
    sedona = get_sedona("03_ndvi_zonal")
    assert_versions(sedona)
    from ndvi_udf import ndvi_masked

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
    red = load_band(sedona, list(zip(todo.scene_id, todo.red_href)), RASTER["tile_px"], "red")
    nir = load_band(sedona, list(zip(todo.scene_id, todo.nir_href)), RASTER["tile_px"], "nir")
    scl = load_band(sedona, list(zip(todo.scene_id, todo.scl_href)), RASTER["scl_tile_px"], "scl")

    meta = sedona.createDataFrame(todo.drop(columns=["red_href", "nir_href", "scl_href"]))
    fields = transformed_fields(sedona, sorted({int(e) for e in todo.epsg}))

    stacked = (red.join(nir, ["scene_id", "x", "y"]).join(scl, ["scene_id", "x", "y"])
               .join(F.broadcast(meta), "scene_id")
               # hand-rolled predicate pushdown: the reader has no spatial pushdown,
               # so semi-join tiles against the broadcast fields BEFORE the map
               # algebra — tiles with no fields never pay for NDVI at all
               .join(F.broadcast(fields),
                     F.expr("RS_Intersects(red, geom) AND f_epsg = epsg"), "left_semi")
               .withColumn("scl10", F.expr("RS_ReprojectMatch(scl, red, 'NearestNeighbor')")))

    # Band values arrive ALREADY as surface reflectance: the GeoTools read path
    # applies the COG's internal scale/offset tags (verified empirically — red
    # tile mean 0.058, range 0.005-0.504). The manifest's scale/offset columns
    # are provenance + a DQ uniformity check, NOT pipeline inputs; applying them
    # again produces NDVI ~= -0.0002 everywhere (caught by the Block-1 gate).
    if RASTER.get("ndvi_engine", "jiffle") == "jiffle":
        from config import QUALITY
        bad = " || ".join(f"(m == {c})" for c in QUALITY["scl_mask_classes"])
        script = (f"m = rast[2]; rr = rast[0]; nn = rast[1]; "
                  f"d = nn + rr; out = con(({bad}) || (d == 0), -9999.0, (nn - rr) / d);")
        tiles = (stacked
                 .withColumn("stack3", F.expr(
                     "RS_AddBand(RS_AddBand(red, nir), scl10)"))
                 .withColumn("ndvi", F.expr(f"RS_MapAlgebra(stack3, 'D', '{script}', -9999.0d)")))
    else:
        tiles = stacked.withColumn("ndvi", ndvi_masked("red", "nir", "scl10"))
    tiles = tiles.select("scene_id", "mgrs_tile", "date", "dekad", "season", "epsg", "x", "y", "ndvi")

    joined = (tiles.join(F.broadcast(fields), F.expr("RS_Intersects(ndvi, geom)"))
              .filter(F.col("f_epsg") == F.col("epsg"))
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
    try:
        writer.append()
    except Exception:
        writer.create()

    n = result.count()
    dt = time.time() - t0
    print(f"field_ndvi: +{n:,} rows, {len(todo)} scenes in {dt:.0f}s "
          f"({dt/len(todo):.0f}s/scene) — capacity-model constant")


if __name__ == "__main__":
    main()
