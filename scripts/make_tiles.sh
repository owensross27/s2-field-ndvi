#!/usr/bin/env bash
# GeoJSONSeq -> PMTiles. Fails the build past 90MB (GitHub hard-blocks 100MiB).
# drop-densest (not coalesce): at mvp scale (278K fields) coalescing cannot fit
# the 500KB/tile budget at z6-7, where individual fields are sub-pixel anyway.
set -euo pipefail
cd "$(dirname "$0")/.."

tippecanoe -o web/fields.pmtiles -l fields -Z6 -z12 --force \
  --drop-densest-as-needed --extend-zooms-if-still-dropping \
  data/publish/fields.geojsonl

SZ=$(stat -f%z web/fields.pmtiles 2>/dev/null || stat -c%s web/fields.pmtiles)
MB=$((SZ / 1000000))
echo "fields.pmtiles: ${MB} MB"
[ "$MB" -lt 90 ] || { echo "FAIL: pmtiles ${MB}MB >= 90MB budget"; exit 1; }
