"""One-time: 3.5GB national CSB GDB zip -> data/iowa_fields.parquet (~300MB).

Run once (anywhere with disk), publish the parquet as a GitHub Release asset,
delete the raw zip/GDB. Reads with pyogrio's SQL filter so only Iowa rows are
materialized. CSBID chars: 1-2 state FIPS, 3-4 start yr, 5-6 end yr, rest seq.
"""
import sys
import zipfile
from pathlib import Path

import pyogrio

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from config import DATA_DIR, FIELDS

ZIP = DATA_DIR / "NationalCSB_2018-2025_rev23.zip"
OUT = DATA_DIR / "iowa_fields.parquet"


def main() -> None:
    workdir = DATA_DIR / "tmp"
    workdir.mkdir(parents=True, exist_ok=True)
    first = next(n for n in zipfile.ZipFile(ZIP).namelist() if ".gdb/" in n)
    gdb_path = workdir / first[: first.index(".gdb/") + 4]
    if not gdb_path.exists():
        print(f"unzipping {ZIP.name} -> {workdir} (one-time, ~10GB transient)")
        zipfile.ZipFile(ZIP).extractall(workdir)

    layers = pyogrio.list_layers(gdb_path)
    layer = layers[0][0]
    print(f"reading layer {layer} with CSBID LIKE '{FIELDS['state_fips']}%'")
    gdf = pyogrio.read_dataframe(
        gdb_path, layer=layer,
        where=f"CSBID LIKE '{FIELDS['state_fips']}%'",
        use_arrow=True,
    )
    print(f"iowa rows: {len(gdf):,}; crs: {gdf.crs}")
    assert len(gdf) > 500_000, "Iowa should have >500K CSB polygons"
    gdf.to_parquet(OUT, compression="zstd")
    print(f"wrote {OUT} ({OUT.stat().st_size/1e6:.0f} MB)")
    print(f"cleanup: rm -rf {workdir} {ZIP}")


if __name__ == "__main__":
    main()
