# Architecture

One pipeline, three consumption surfaces. Every stage is open-source Apache Spark +
Apache Sedona; scheduling and compute are interchangeable because runs are idempotent
over (date, mgrs_tile) partitions.

```mermaid
flowchart TD
  subgraph SRC["Sources (all free, anonymous, public domain)"
    ]
    A1["Earth Search STAC API<br/>sentinel-2-c1-l2a COGs"]
    A2["USDA CSB 2018-2025<br/>631K Iowa field polygons"]
    A3["USDA derecho wind-gust polygons<br/>+ TIGER counties"]
    A4["Copernicus DEM 30m<br/>(context, planned)"]
  end

  A1 --> B["02_scenes: STAC search, dedupe,<br/>s3a href rewrite, per-asset scale/offset"]
  B --> C[("Iceberg crop.scenes<br/>immutable scene manifest")]
  A2 --> D["01_fields: Iowa filter, -15m buffer,<br/>per-UTM ST_Transform + ST_SetSRID"]
  A3 --> D
  D --> E[("Iceberg crop.fields (WKB)<br/>+ wind_zones + counties")]

  C --> F["03_ndvi_zonal<br/>format('raster') 256px tiles, s3a range reads<br/>semi-join pushdown vs broadcast fields<br/>RS_ReprojectMatch SCL to 10m<br/>NDVI: jiffle (JVM) or python raster UDF<br/>RS_ZonalStatsAll once per (tile, field)"]
  E --> F
  F --> G[("Iceberg crop.field_ndvi<br/>partitioned by (date, mgrs_tile)<br/>= restart + refresh unit")]

  G --> H["05_dq: Great Expectations gate<br/>+ run_metrics (planned)"]
  H --> I["04_publish: wide pivot,<br/>uint8 quantization, 255 = masked"]

  I --> J["PMTiles + MapLibre<br/>static map, GitHub Pages"]
  I --> K["GeoParquet drop<br/>DuckDB / Ask-the-Fields NL-to-SQL"]
  G --> L["ML handoff<br/>event-labeled feature table (documented)"]
  G --> M["derecho_event_study.ipynb<br/>difference-in-differences vs wind class"]
```

## Compute targets (one image)

| Target | What it proves | Engine |
|---|---|---|
| laptop local[4] | reviewer reproducibility, $0 | jiffle |
| kind + spark-submit | real K8s manifests, $0 | jiffle |
| EC2 spot us-west-2 | state scale, measured $ | jiffle |
| EKS + spark-operator | distributed Spark ops | python_udf vs jiffle benchmark |
| ECS Fargate via OIDC | credential-less scheduled CI runs | jiffle |

## Data contracts

- `crop.scenes`: one row per STAC item; `selected` marks best-per-(tile, dekad)
  (season mode) or best-per-(tile, date) (event mode). The manifest is the
  reproducibility boundary: STAC is live and mutable, this table is not.
- `crop.field_ndvi`: (field_id, date, dekad, season, mgrs_tile, utm_zone, scene_id,
  mean_ndvi, valid_px, total_px, valid_frac). Bands join by scene_id, so cross-date
  stitching is impossible by construction.
- Geometry is WKB in every Iceberg table: Sedona GeometryUDT cannot be written to
  Iceberg on the Spark 3.5 line (native geometry lands with Spark 4.1 + Iceberg v3).
