"""CSB Iowa fields + wind polygons + counties -> Iceberg (local.crop.*).

All geometry columns are WKB (ST_AsBinary) — Iceberg on this stack cannot store
GeometryUDT. Buffering happens in EPSG:5070 (meters, native CRS of CSB); UTM
transforms are explicit with ST_SetSRID on both ends (SRID-0 silently
double-transforms — see docs/build-plan.md).
"""
import sys
from pathlib import Path

import geopandas as gpd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DATA_DIR, FIELDS, ICEBERG, scope
from session import assert_versions, get_sedona

CAT = ICEBERG["catalog"]


def pick_demo_county(wind: gpd.GeoDataFrame, counties: gpd.GeoDataFrame) -> str:
    """County (FIPS) with the largest 100+ mph wind-swath overlap.

    Run once when scopes.demo.county_fips is null; pin the result in config.yml.
    """
    top = wind[wind["gust_class"] == wind["gust_class"].max()].to_crs(5070)
    ia = counties[counties["STATEFP"] == FIELDS["state_fips"]].to_crs(5070)
    overlap = (
        gpd.overlay(ia, top[["geometry"]], how="intersection")
        .assign(a=lambda d: d.geometry.area)
        .groupby("GEOID")["a"].sum().sort_values(ascending=False)
    )
    print("top counties by 100+ mph overlap (m2):")
    print(overlap.head(5).to_string())
    return str(overlap.index[0])


def load_small_layers(sedona) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    wind_dir = DATA_DIR / "wind_polygons"
    shp = next(wind_dir.rglob("*.shp"))
    wind = gpd.read_file(shp)
    # USDA ships exactly one attribute: WindGust in {"60-79 mph","80-99 mph","100+ mph"}
    order = {"60-79 mph": 1, "80-99 mph": 2, "100+ mph": 3}
    wind["gust_class"] = wind["WindGust"].map(order)
    wind["gust_label"] = wind["WindGust"]
    assert wind["gust_class"].notna().all(), f"unexpected WindGust values: {wind.WindGust.unique()}"

    counties = gpd.read_file(next((DATA_DIR / "counties_500k").rglob("*.shp")))

    for name, gdf in [("wind_zones", wind.to_crs(4326)),
                      ("counties", counties[counties.STATEFP == FIELDS["state_fips"]].to_crs(4326))]:
        pdf = gdf.drop(columns="geometry").assign(wkb_4326=gdf.geometry.to_wkb())
        sedona.createDataFrame(pdf).writeTo(f"{CAT}.crop.{name}").createOrReplace()
        print(f"{name}: {len(pdf)} rows")
    return wind, counties


def main() -> None:
    sc = scope()
    sedona = get_sedona("01_fields")
    assert_versions(sedona)
    sedona.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.crop")

    wind, counties = load_small_layers(sedona)
    county_fips = sc.get("county_fips") or pick_demo_county(wind, counties)
    print(f"demo county FIPS: {county_fips} (pin this in config.yml scopes.demo)")

    fields = sedona.read.format("geoparquet").load(str(DATA_DIR / "iowa_fields.parquet"))
    fields.createOrReplaceTempView("raw")

    cdl_cols = [c for c in fields.columns if c.upper().startswith("CDL")]
    cdl_sel = ", ".join(cdl_cols) if cdl_cols else ""
    county_filter = ""
    if sc["name"] == "demo":
        cty = counties[counties.GEOID == county_fips].to_crs(5070).geometry.union_all()
        county_filter = ("AND ST_Intersects(ST_Centroid(geometry), "
                         f"ST_SetSRID(ST_GeomFromWKB(X'{cty.wkb.hex()}'), 5070)) ")

    df = sedona.sql(f"""
        WITH g AS (
          SELECT CSBID AS field_id, {cdl_sel + ',' if cdl_sel else ''}
                 ST_SetSRID(geometry, 5070) AS geom_5070,
                 ST_Area(geometry) AS area_m2
          FROM raw
          WHERE ST_Area(geometry) >= {FIELDS['min_area_m2']} {county_filter}
        ),
        b AS (
          SELECT *, ST_Buffer(geom_5070, {FIELDS['buffer_m']}) AS geom_buf_5070,
                 ST_Transform(geom_5070, 'EPSG:5070', 'EPSG:4326') AS geom_4326
          FROM g WHERE NOT ST_IsEmpty(ST_Buffer(geom_5070, {FIELDS['buffer_m']}))
        )
        SELECT field_id, {cdl_sel + ',' if cdl_sel else ''} area_m2,
               CAST(FLOOR((ST_X(ST_Centroid(geom_4326)) + 180) / 6) + 1 AS INT) AS utm_zone,
               ST_AsBinary(geom_4326) AS geom_4326_wkb,
               ST_AsBinary(geom_buf_5070) AS geom_buf_5070_wkb
        FROM b
    """)
    df.writeTo(f"{CAT}.crop.fields").partitionedBy("utm_zone").createOrReplace()
    n = sedona.table(f"{CAT}.crop.fields").count()
    print(f"fields written: {n:,}")
    assert n > (5_000 if sc["name"] == "demo" else 400_000), f"suspiciously few fields: {n}"


if __name__ == "__main__":
    main()
