"""field_ndvi + fields -> web-ready GeoJSONSeq (for tippecanoe) + GeoParquet drop.

Wide per-field pivot with uint8-quantized NDVI props for tiles (255 = masked,
valid_frac below threshold renders grey, never a fake value); full-precision
floats go to the GeoParquet drop for DuckDB/agent consumption.
"""
import json
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkb as shapely_wkb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DATA_DIR, EVENT, ICEBERG, QUALITY, RASTER, REPO_ROOT, scope
from session import get_sedona

CAT = ICEBERG["catalog"]
OUT = DATA_DIR / "publish"
CLAMP_LO, CLAMP_HI = RASTER["ndvi_clamp"]
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def quant(v: pd.Series, valid: pd.Series) -> np.ndarray:
    q = np.clip((v - CLAMP_LO) / (CLAMP_HI - CLAMP_LO), 0, 1) * 200
    q = q.round()
    bad = v.isna() | (valid < QUALITY["valid_frac_min"])
    return np.where(bad, 255, q).astype(np.uint8)


def publish_season(sedona, gdf: gpd.GeoDataFrame) -> list[str]:
    """Adds quantized d{dekad} columns to gdf in place, writes web/season.json.

    Publishes the largest scope() season that actually has field_ndvi rows
    (not hardcoded to the event season -- once seasons beyond 2020 land this
    picks the newest one automatically).
    """
    present = {r.season for r in
               sedona.sql(f"SELECT DISTINCT season FROM {CAT}.crop.field_ndvi").collect()}
    candidates = [s for s in scope()["seasons"] if s in present]
    if not candidates:
        print(f"no field_ndvi rows for scope seasons {scope()['seasons']}, skipping season publish")
        # write the empty payload so a stale web/season.json (advertising d*
        # props the freshly-rebuilt tileset no longer has) never survives a
        # scope with no season data -- app.js treats an empty dekads list as
        # "no season" and hides the button.
        (REPO_ROOT / "web" / "season.json").write_text(json.dumps({"season": None, "dekads": []}, indent=2))
        return []
    season = max(candidates)

    # MAX(...) GROUP BY collapses to one row per (field_id, dekad) -- a field
    # with no scene that dekad just has no row here, and pivot leaves it NaN
    # -> quant() -> 255. Overlapping MGRS tiles (mvp scope) put >1 row in a
    # group, so mean_ndvi and valid_frac must come off the SAME row -- two
    # independent MAX()s could pair a cloud-rejected mean_ndvi with another
    # row's passing valid_frac. MAX(struct(...)) picks one winning row (by
    # valid_frac) and reads both fields off it.
    #
    # ponytail: narrow is toPandas()'d and pivoted on the driver (fields x
    # dekads); pivot also adds one uint8 tile property per dekad, and the
    # 90MB tippecanoe budget in scripts/make_tiles.sh is a hard exit(1), not
    # a degrade. Fine at demo scope (13k rows, 2 dekads); revisit with a
    # Spark-side pivot or a cap on published dekads before mvp scope
    # (~16 dekads x full field count).
    narrow = sedona.sql(f"""
        SELECT field_id, dekad,
               MAX(struct(valid_frac, mean_ndvi)).mean_ndvi AS mean_ndvi,
               MAX(struct(valid_frac, mean_ndvi)).valid_frac AS valid_frac
        FROM {CAT}.crop.field_ndvi
        WHERE season = {season}
        GROUP BY field_id, dekad
    """).toPandas()
    pivot = narrow.pivot(index="field_id", columns="dekad", values=["mean_ndvi", "valid_frac"])

    dekad_props = []
    for d in sorted(narrow["dekad"].unique()):
        prop = f"d{d}"
        ndvi = pd.Series(pivot[("mean_ndvi", d)].reindex(gdf["field_id"]).to_numpy())
        vf = pd.Series(pivot[("valid_frac", d)].reindex(gdf["field_id"]).to_numpy())
        gdf[prop] = quant(ndvi, vf.fillna(0))
        dekad_props.append(prop)

    # earliest selected-scene date per dekad, across tiles -- tiles pick their
    # own best scene independently, so two tiles can land days apart in the
    # same dekad; season.json's date/label is the min, a slider label not a
    # per-tile-exact date. Constraint this relies on: event pre/post dates
    # must be >= dekad_days apart, or 02_scenes.py's per-(tile, date) pick
    # collapses both into one dekad and this label silently reports whichever
    # date MAX(struct(valid_frac,...)) happened to favor, not necessarily the
    # earlier one.
    dates = sedona.sql(f"""
        SELECT dekad, MIN(date) AS min_date FROM {CAT}.crop.field_ndvi
        WHERE season = {season} GROUP BY dekad ORDER BY dekad
    """).toPandas()
    season_json = {
        "season": int(season),
        "dekads": [
            {"prop": f"d{int(row.dekad)}",
             "date": pd.Timestamp(row.min_date).strftime("%Y-%m-%d"),
             # %b is locale-dependent (LC_TIME); this is a committed, English-only
             # map artifact, so index a literal table instead of strftime("%b").
             "label": f"{MONTHS[pd.Timestamp(row.min_date).month - 1]} {pd.Timestamp(row.min_date).day:02d}"}
            for row in dates.itertuples()
        ],
    }
    (REPO_ROOT / "web" / "season.json").write_text(json.dumps(season_json, indent=2))
    print(f"season {season} of {candidates}: {dekad_props} -> web/season.json")
    return dekad_props


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

    # full-precision queryable drop (DuckDB / Ask-the-Fields substrate) --
    # event pair only, same as today; season dekads don't get a parquet drop
    gdf.drop(columns=["fid"]).to_parquet(OUT / "field_ndvi.parquet", compression="zstd")

    dekad_props = publish_season(sedona, gdf)

    tile_cols = ["fid", "crop", "wind", "pre_q", "post_q", "drop_q", *dekad_props, "geometry"]
    gdf[tile_cols].to_file(OUT / "fields.geojsonl", driver="GeoJSONSeq")

    counties = gpd.read_file(next((DATA_DIR / "counties_500k").rglob("*.shp")))
    counties[counties.GEOID == "19011"].to_crs(4326)[["geometry"]] \
        .to_file(REPO_ROOT / "web" / "county.geojson", driver="GeoJSON")
    print(f"wrote {OUT}/fields.geojsonl, {OUT}/field_ndvi.parquet, web/county.geojson")


if __name__ == "__main__":
    main()
