"""Block-1 exit gate: corn mean NDVI on 2020-08-04 in [0.6, 0.9], plus a first
uncontrolled look at NDVI drop by wind class (the DiD notebook does this properly)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from config import ICEBERG
from session import get_sedona

CAT = ICEBERG["catalog"]
sedona = get_sedona("gate_check")

corn = sedona.sql(f"""
    SELECT n.date, COUNT(*) AS fields, ROUND(AVG(n.mean_ndvi), 4) AS corn_mean_ndvi,
           ROUND(AVG(n.valid_frac), 3) AS avg_valid_frac
    FROM {CAT}.crop.field_ndvi n JOIN {CAT}.crop.fields f USING (field_id)
    WHERE f.CDL2020 = 1 AND n.valid_frac >= 0.5
    GROUP BY n.date ORDER BY n.date
""").collect()
for r in corn:
    print(f"corn {r.date}: {r.fields} fields, mean NDVI {r.corn_mean_ndvi}, valid_frac {r.avg_valid_frac}")

pre = next(r.corn_mean_ndvi for r in corn if str(r.date) == "2020-08-04")
assert 0.6 <= pre <= 0.9, f"GATE FAILED: pre-derecho corn NDVI {pre} outside [0.6, 0.9]"
print(f"GATE PASSED: pre-derecho corn NDVI = {pre}")

print("\nNDVI drop by wind class (raw, drought-confounded — DiD comes later):")
sedona.sql(f"""
    WITH wide AS (
      SELECT n.field_id,
             MAX(CASE WHEN n.date = DATE'2020-08-04' THEN n.mean_ndvi END) AS pre,
             MAX(CASE WHEN n.date = DATE'2020-08-19' THEN n.mean_ndvi END) AS post
      FROM {CAT}.crop.field_ndvi n
      WHERE n.valid_frac >= 0.5 GROUP BY n.field_id
      HAVING pre IS NOT NULL AND post IS NOT NULL
    ),
    zoned AS (
      SELECT w.*, COALESCE(MAX(z.gust_class), 0) AS gust_class
      FROM wide w
      JOIN {CAT}.crop.fields f ON w.field_id = f.field_id
      LEFT JOIN {CAT}.crop.wind_zones z
        ON ST_Intersects(ST_Centroid(ST_GeomFromWKB(f.geom_4326_wkb)),
                         ST_GeomFromWKB(z.wkb_4326))
      WHERE f.CDL2020 = 1
      GROUP BY w.field_id, w.pre, w.post
    )
    SELECT gust_class, COUNT(*) AS fields,
           ROUND(AVG(pre), 4) AS pre, ROUND(AVG(post), 4) AS post,
           ROUND(AVG(post - pre), 4) AS drop
    FROM zoned GROUP BY gust_class ORDER BY gust_class
""").show()
