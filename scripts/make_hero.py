"""Hero visuals: derecho before/after NDVI PNGs + a flip GIF over Benton County.

Reads COG windows straight off S3 with rasterio (AWS_NO_SIGN_REQUEST). rasterio
does NOT auto-apply the COG's scale/offset tags (unlike the Sedona/GeoTools read
path), so reflectance = DN * scales[0] + offsets[0] is applied here explicitly.
"""
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import imageio.v3 as iio
import matplotlib.pyplot as plt
import numpy as np
import rasterio
from rasterio.warp import transform_bounds
from rasterio.windows import from_bounds

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from config import DATA_DIR, FIELDS, REPO_ROOT

IMG = REPO_ROOT / "docs" / "img"
MASK_SCL = {0, 1, 3, 8, 9, 10, 11}

os.environ.setdefault("AWS_NO_SIGN_REQUEST", "YES")


def county_bounds_4326():
    import geopandas as gpd
    counties = gpd.read_file(next((DATA_DIR / "counties_500k").rglob("*.shp")))
    cty = counties[counties.GEOID == "19011"].to_crs(4326)
    return tuple(cty.total_bounds)


def read_ndvi(red_href: str, nir_href: str, scl_href: str, bounds4326):
    def s3url(href):
        return href.replace("s3a://", "s3://")
    with rasterio.open(s3url(red_href)) as red_ds:
        bounds = transform_bounds("EPSG:4326", red_ds.crs, *bounds4326)
        win = from_bounds(*bounds, red_ds.transform).round_offsets().round_lengths()
        red = red_ds.read(1, window=win).astype(np.float64)
        red = red * red_ds.scales[0] + red_ds.offsets[0]
    with rasterio.open(s3url(nir_href)) as nir_ds:
        nir = nir_ds.read(1, window=win).astype(np.float64)
        nir = nir * nir_ds.scales[0] + nir_ds.offsets[0]
    with rasterio.open(s3url(scl_href)) as scl_ds:
        swin = from_bounds(*bounds, scl_ds.transform).round_offsets().round_lengths()
        scl = scl_ds.read(1, window=swin, out_shape=red.shape)  # nearest upsample
    denom = nir + red
    with np.errstate(invalid="ignore", divide="ignore"):
        ndvi = (nir - red) / denom
    bad = np.isin(scl, list(MASK_SCL)) | (denom == 0)
    return np.where(bad, np.nan, ndvi)


def render(ndvi, title, out_png):
    fig, ax = plt.subplots(figsize=(7, 7), dpi=110)
    cmap = plt.get_cmap("RdYlGn").copy()
    cmap.set_bad("#d9d9d9")
    im = ax.imshow(ndvi, cmap=cmap, vmin=0.0, vmax=0.95)
    ax.set_title(title, fontsize=12)
    ax.axis("off")
    fig.colorbar(im, ax=ax, shrink=0.7, label="NDVI (grey = cloud/nodata)")
    fig.tight_layout()
    fig.savefig(out_png, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_png}")


def main() -> None:
    IMG.mkdir(parents=True, exist_ok=True)
    import duckdb
    from config import scope
    tiles = ",".join(f"'{t}'" for t in scope()["tiles"])
    # raw Iceberg data files include superseded snapshots: filter to scope tiles
    # and keep only the newest row per (scene) — cheap stand-in for snapshot reads
    scenes = duckdb.sql(f"""
        SELECT * FROM read_parquet('{REPO_ROOT}/warehouse/crop/scenes/data/**/*.parquet')
        WHERE selected AND mgrs_tile IN ({tiles})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY scene_id ORDER BY ingested_at DESC) = 1
        ORDER BY date
    """).df()
    b = county_bounds_4326()
    frames = []
    for _, s in scenes.iterrows():
        ndvi = read_ndvi(s.red_href, s.nir_href, s.scl_href, b)
        # downsample for a manageable image (county window at 10m is ~4-5K px wide)
        step = max(1, ndvi.shape[1] // 1200)
        small = ndvi[::step, ::step]
        label = "before, 2020-08-04" if str(s.date) < "2020-08-10" else "after, 2020-08-19"
        png = IMG / f"derecho_{'pre' if 'before' in label else 'post'}.png"
        render(small, f"Benton County corn belt NDVI: {label}", png)
        frames.append(iio.imread(png))
    h = min(f.shape[0] for f in frames)
    w = min(f.shape[1] for f in frames)
    iio.imwrite(IMG / "hero.gif", [f[:h, :w] for f in frames], duration=1200, loop=0)
    sz = (IMG / "hero.gif").stat().st_size / 1e6
    print(f"wrote {IMG / 'hero.gif'} ({sz:.1f} MB)")
    assert sz < 5, "hero.gif exceeds the 5MB README budget"


if __name__ == "__main__":
    main()
