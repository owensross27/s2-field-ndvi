"""field_ndvi + fields -> web-ready GeoJSONSeq (for tippecanoe) + GeoParquet drop.

Wide per-field pivot with uint8-quantized NDVI props for tiles (255 = masked,
valid_frac below threshold renders grey, never a fake value); full-precision
floats go to the GeoParquet drop for DuckDB/agent consumption.
"""
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkb as shapely_wkb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DATA_DIR, EVENT, ICEBERG, QUALITY, RASTER, REPO_ROOT
from session import get_sedona

CAT = ICEBERG["catalog"]
OUT = DATA_DIR / "publish"
CLAMP_LO, CLAMP_HI = RASTER["ndvi_clamp"]


def quant(v: pd.Series, valid: pd.Series) -> np.ndarray:
    q = np.clip((v - CLAMP_LO) / (CLAMP_HI - CLAMP_LO), 0, 1) * 200
    q = q.round()
    bad = v.isna() | (valid < QUALITY["valid_frac_min"])
    return np.where(bad, 255, q).astype(np.uint8)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    sedona = get_sedona("04_publish")

    df = sedona.sql(f"""
        WITH wide AS (
          SELECT field_id,
                 MAX(CASE WHEN date = DATE'{EVENT["pre_date"]}'  THEN mean_ndvi  END) AS pre,
                 MAX(CASE WHEN date = DATE'{EVENT["pre_date"]}'  THEN valid_frac END) AS pre_vf,
                 MAX(CASE WHEN date = DATE'{EVENT["post_date"]}' THEN mean_ndvi  END) AS post,
                 MAX(CASE WHEN date = DATE'{EVENT["post_date"]}' THEN valid_frac END) AS post_vf
          FROM {CAT}.crop.field_ndvi GROUP BY field_id
        )
        SELECT w.*, f.CDL2020 AS crop, f.area_m2, f.geom_4326_wkb,
               COALESCE(MAX(z.gust_class), 0) AS wind
        FROM wide w
        JOIN {CAT}.crop.fields f USING (field_id)
        LEFT JOIN {CAT}.crop.wind_zones z
          ON ST_Intersects(ST_Centroid(ST_GeomFromWKB(f.geom_4326_wkb)),
                           ST_GeomFromWKB(z.wkb_4326))
        GROUP BY w.field_id, w.pre, w.pre_vf, w.post, w.post_vf,
                 f.CDL2020, f.area_m2, f.geom_4326_wkb
    """).toPandas()
    print(f"publish rows: {len(df):,}")

    gdf = gpd.GeoDataFrame(
        df.drop(columns=["geom_4326_wkb"]),
        geometry=[shapely_wkb.loads(bytes(b)) for b in df.geom_4326_wkb],
        crs=4326,
    )
    gdf["fid"] = np.arange(len(gdf), dtype=np.int32)
    gdf["pre_q"] = quant(gdf.pre, gdf.pre_vf.fillna(0))
    gdf["post_q"] = quant(gdf.post, gdf.post_vf.fillna(0))
    drop = gdf.post - gdf.pre
    # drop ramp: [-0.4, +0.1] -> 0..200 (blue-to-red diverging on the client)
    dq = np.clip((drop - (-0.4)) / 0.5, 0, 1) * 200
    bad = drop.isna() | (gdf.pre_vf.fillna(0) < QUALITY["valid_frac_min"]) \
        | (gdf.post_vf.fillna(0) < QUALITY["valid_frac_min"])
    gdf["drop_q"] = np.where(bad, 255, dq.round()).astype(np.uint8)

    # full-precision queryable drop (DuckDB / Ask-the-Fields substrate)
    gdf.drop(columns=["fid"]).to_parquet(OUT / "field_ndvi.parquet", compression="zstd")

    tile_cols = ["fid", "crop", "wind", "pre_q", "post_q", "drop_q", "geometry"]
    gdf[tile_cols].to_file(OUT / "fields.geojsonl", driver="GeoJSONSeq")

    counties = gpd.read_file(next((DATA_DIR / "counties_500k").rglob("*.shp")))
    counties[counties.GEOID == "19011"].to_crs(4326)[["geometry"]] \
        .to_file(REPO_ROOT / "web" / "county.geojson", driver="GeoJSON")
    print(f"wrote {OUT}/fields.geojsonl, {OUT}/field_ndvi.parquet, web/county.geojson")


if __name__ == "__main__":
    main()
