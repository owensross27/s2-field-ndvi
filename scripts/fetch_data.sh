#!/usr/bin/env bash
# Fetches the small inputs. The big one (CSB 3.5GB national GDB) is only needed
# once to produce data/iowa_fields.parquet; once the repo is public that file
# ships as a GitHub Release asset and this script downloads it instead.
set -euo pipefail
cd "$(dirname "$0")/.." && mkdir -p data && cd data

fetch() {  # fetch <url> <out> — skip if present
  [ -f "$2" ] && { echo "have $2"; return; }
  curl -sL --fail -o "$2" "$1" && echo "fetched $2"
}

fetch "https://www.nass.usda.gov/Research_and_Science/Disaster-Analysis/2020/Iowa_Derecho/Derecho_Iowa_082520_WindGust_Polygons.zip" wind_polygons.zip
fetch "https://www2.census.gov/geo/tiger/GENZ2025/shp/cb_2025_us_county_500k.zip" counties_500k.zip

for z in wind_polygons counties_500k; do
  [ -d "$z" ] || (mkdir -p "$z" && unzip -qo "$z.zip" -d "$z")
done

if [ ! -f iowa_fields.parquet ]; then
  if [ -f NationalCSB_2018-2025_rev23.zip ]; then
    echo "CSB zip present; run scripts/extract_csb.py to produce iowa_fields.parquet"
  else
    echo "MISSING iowa_fields.parquet - download the Release asset or the 3.5GB CSB zip"
    echo "  https://www.nass.usda.gov/Research_and_Science/Crop-Sequence-Boundaries/datasets/NationalCSB_2018-2025_rev23.zip"
    exit 1
  fi
fi
echo "data ready"
