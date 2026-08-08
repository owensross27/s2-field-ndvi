"""Diagnose the empty field_ndvi: inspect name values, tile counts, join cardinalities."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from config import ICEBERG, RASTER
from session import get_sedona

from pyspark.sql import functions as F

CAT = ICEBERG["catalog"]
sedona = get_sedona("debug_join")

scenes = sedona.table(f"{CAT}.crop.scenes").filter("selected").toPandas()
red_href = scenes.red_href.iloc[0]
scl_href = scenes.scl_href.iloc[0]
print("red href:", red_href)

red = (sedona.read.format("raster")
       .option("tileWidth", str(RASTER["tile_px"])).option("tileHeight", str(RASTER["tile_px"]))
       .load([red_href]))
print("red schema:", red.schema.simpleString())
red.select("name", "x", "y").show(3, truncate=False)
print("red tiles:", red.count(), "| distinct x:", red.select("x").distinct().count())

scl = (sedona.read.format("raster")
       .option("tileWidth", str(RASTER["scl_tile_px"])).option("tileHeight", str(RASTER["scl_tile_px"]))
       .load([scl_href]))
print("scl tiles:", scl.count(), "| distinct x:", scl.select("x").distinct().count())

j = red.select("x", "y").join(scl.select("x", "y"), ["x", "y"])
print("red-scl (x,y) join:", j.count())

one = red.limit(1).withColumn("env", F.expr("RS_Envelope(rast)")) \
    .withColumn("srid", F.expr("RS_SRID(rast)"))
one.select("srid", F.expr("ST_AsText(env)").alias("env")).show(truncate=False)

flds = sedona.sql(f"""
    SELECT ST_SetSRID(ST_Transform(ST_SetSRID(ST_GeomFromWKB(geom_buf_5070_wkb), 5070),
           'EPSG:5070', 'EPSG:32615'), 32615) AS geom
    FROM {CAT}.crop.fields LIMIT 5""")
flds.select(F.expr("ST_AsText(ST_Centroid(geom))").alias("c")).show(truncate=False)
