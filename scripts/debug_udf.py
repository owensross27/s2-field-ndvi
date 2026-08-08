"""Single-tile UDF repro: if one row crashes the worker, it's serde, not resources."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from config import ICEBERG, RASTER
from session import get_sedona

from pyspark.sql import functions as F

CAT = ICEBERG["catalog"]
sedona = get_sedona("debug_udf")
from ndvi_udf import ndvi_masked

s = sedona.table(f"{CAT}.crop.scenes").filter("selected").toPandas().iloc[0]

def one(href, px, col):
    return (sedona.read.format("raster")
            .option("tileWidth", str(px)).option("tileHeight", str(px)).load(href)
            .filter("x = 20 AND y = 20")
            .select("x", "y", F.col("rast").alias(col)))

red = one(s.red_href, RASTER["tile_px"], "red")
nir = one(s.nir_href, RASTER["tile_px"], "nir")
scl = one(s.scl_href, RASTER["scl_tile_px"], "scl")

row = (red.join(nir, ["x", "y"]).join(scl, ["x", "y"])
       .withColumn("scl10", F.expr("RS_ReprojectMatch(scl, red, 'NearestNeighbor')"))
       .withColumn("ndvi", ndvi_masked("red", "nir", "scl10",
                                       F.lit(float(s.red_scale)), F.lit(float(s.red_offset)),
                                       F.lit(float(s.nir_scale)), F.lit(float(s.nir_offset))))
       .withColumn("stats", F.expr(
           "RS_ZonalStatsAll(ndvi, ST_SetSRID(RS_Envelope(ndvi), RS_SRID(ndvi)), 1)")))
print(row.select("x", "y", "stats").collect())
print("single-tile UDF OK")
