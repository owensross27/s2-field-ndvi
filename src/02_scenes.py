"""STAC -> local.crop.scenes: the immutable, reproducible scene manifest.

Queries Earth Search for the scope's tiles/dates, captures per-asset hrefs
(rewritten https -> s3a; Hadoop's HttpFileSystem cannot seek) and per-asset
scale/offset from raster:bands (never hardcoded: NDVI is not offset-invariant).
Uses the plain STAC API via requests on the driver for the manifest — the scene
list is small; the heavy lifting stays in Spark downstream.
"""
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import ICEBERG, QUALITY, STAC, TIME, epsg_for_tile, scope
from session import assert_versions, get_sedona

CAT = ICEBERG["catalog"]
S3A = {
    "https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/":
        "s3a://e84-earth-search-sentinel-data/",
    "https://sentinel-cogs.s3.us-west-2.amazonaws.com/": "s3a://sentinel-cogs/",
}


def to_s3a(href: str) -> str:
    for prefix, repl in S3A.items():
        if href.startswith(prefix):
            return href.replace(prefix, repl, 1)
    raise ValueError(f"unrecognized asset host (add to S3A map): {href}")


def dekad_of(date_iso: str) -> int:
    d = datetime.fromisoformat(date_iso[:10])
    return (d.timetuple().tm_yday - 1) // TIME["dekad_days"]


def search_items(tiles: list[str], date_ranges: list[str]) -> list[dict]:
    items = []
    for dr in date_ranges:
        body = {
            "collections": [STAC["collection"]],
            "datetime": dr,
            "query": {"grid:code": {"in": [f"MGRS-{t}" for t in tiles]}},
            "limit": STAC["items_limit_per_request"],
        }
        url = f"{STAC['url']}/search"
        while url:
            r = requests.post(url, json=body, timeout=60)
            r.raise_for_status()
            page = r.json()
            items += page["features"]
            nxt = [l for l in page.get("links", []) if l.get("rel") == "next"]
            url, body = (nxt[0]["href"], nxt[0].get("body", body)) if nxt else (None, None)
    return items


def row_from_item(it: dict) -> dict:
    p = it["properties"]
    assets = it["assets"]
    def band(key):
        a = assets[key]
        rb = (a.get("raster:bands") or [{}])[0]
        return to_s3a(a["href"]), float(rb.get("scale", 1.0)), float(rb.get("offset", 0.0))
    red_href, red_scale, red_offset = band("red")
    nir_href, nir_scale, nir_offset = band("nir")
    scl_href, _, _ = band("scl")
    tile = re.sub("^MGRS-", "", p["grid:code"])
    date = p["datetime"][:10]
    return dict(
        scene_id=it["id"], mgrs_tile=tile, date=date, dekad=dekad_of(date),
        season=int(date[:4]), cloud_cover=float(p.get("eo:cloud_cover", 100.0)),
        nodata_pct=float(p.get("s2:nodata_pixel_percentage", 0.0)),
        epsg=int(p.get("proj:epsg", epsg_for_tile(tile))),
        red_href=red_href, nir_href=nir_href, scl_href=scl_href,
        red_scale=red_scale, red_offset=red_offset,
        nir_scale=nir_scale, nir_offset=nir_offset,
        ingested_at=datetime.now(timezone.utc).isoformat(),
    )


def date_ranges_for(sc: dict) -> list[str]:
    if "dates" in sc and sc["dates"]:
        return [f"{d}T00:00:00Z/{d}T23:59:59Z" for d in sc["dates"]]
    doy0, doy1 = TIME["growing_season_doy"]
    out = []
    for season in sc["seasons"]:
        start = datetime.strptime(f"{season}-{doy0}", "%Y-%j").date()
        end = datetime.strptime(f"{season}-{doy1}", "%Y-%j").date()
        out.append(f"{start}T00:00:00Z/{end}T23:59:59Z")
    return out


def main() -> None:
    sc = scope()
    sedona = get_sedona("02_scenes")
    assert_versions(sedona)
    items = search_items(sc["tiles"], date_ranges_for(sc))
    print(f"stac items: {len(items)}")
    rows = [row_from_item(it) for it in items]
    df = sedona.createDataFrame(rows)

    from pyspark.sql import Window
    from pyspark.sql import functions as F
    ok = (F.col("cloud_cover") <= QUALITY["cloud_cover_max_pct"]) & \
         (F.col("nodata_pct") <= QUALITY["nodata_max_pct"])
    # Event mode (explicit dates): dates are pinned, so selection is best-per-(tile,
    # date) WITHOUT the scene-level cloud gate — the per-pixel SCL mask plus
    # valid_frac_min does the cleanup. Season mode: gate, then best per dekad.
    event_mode = bool(sc.get("dates"))
    part = ["mgrs_tile", "date"] if event_mode else ["mgrs_tile", "season", "dekad"]
    w = Window.partitionBy(*part).orderBy("cloud_cover")
    best = F.row_number().over(w) == 1
    df = (df.withColumn("usable", ok)
            .withColumn("selected", best if event_mode else (ok & best)))

    sedona.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.crop")
    # dynamic partition overwrite: refresh THIS scope's (season, tile) partitions
    # without wiping other scopes' manifest rows (catch-up diffs depend on them)
    writer = df.writeTo(f"{CAT}.crop.scenes").partitionedBy("season", "mgrs_tile")
    try:
        writer.overwritePartitions()
    except Exception:
        writer.create()
    sel = df.filter("selected").count()
    print(f"scenes written: {df.count()} rows, {sel} selected")
    assert sel > 0, "no scenes selected — check cloud/nodata filters and dates"


if __name__ == "__main__":
    main()
