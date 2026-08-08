# s2-field-ndvi — State-Scale Sedona Raster Lakehouse (Plan)

## Context

Second portfolio repo for the Wherobots Sr. Solution Architect application (first:
`/Users/ross/ais-port-dwell/` — vector/trajectory/Iceberg-schema-evolution, planned in
`/Users/ross/job-hunt/wherobots/build-plan.md`, demo layer built, main build not started).
This repo covers the RASTER half of Sedona's surface: Sentinel-2 NDVI time series → zonal
stats over ~631K USDA field polygons (Iowa) → Iceberg → static PMTiles/MapLibre map on
GitHub Pages + animated GIF hero. Open-sourced on Ross's GitHub. The 2020-08-10 derecho is
the validation case: per-field NDVI drop vs USDA's published wind-gust polygons.

Critical new constraint: Ross has a **14-day Wherobots Cloud Pro trial active now
(expires ~2026-08-21)**. The trial gets ONE bounded 4h block serving BOTH repos.

**Whole pipeline is open-source Apache Spark** (PySpark 3.5 + Sedona 1.9.1) on laptop /
Docker / kind / EC2 / EKS. Wherobots is a quarantined comparison chapter (`wherobots/`
dir, not in the Makefile). This shows both raw-Spark competence AND platform literacy.

## Purpose (the one-liner the repo opens with)

> **An open field-level crop-stress feed: raw Sentinel-2 pixels become a queryable NDVI
> anomaly table for every one of 631K Iowa fields — consumed as a map by humans, as SQL
> by analysts and agents, and as features by damage/yield models — proven by detecting
> and quantifying the 2020 derecho against USDA's wind data.**

Why it matters: after a weather event, insurers/lenders/co-ops need field-level damage
triage in days; USDA's 2020 assessment took weeks and a GIS desk. The lakehouse pattern
here is the answer an SA sells: one pipeline, three consumption surfaces, no GIS desktop.

### Consumption layer (downstream of gold — this is the "why")
1. **Map** (humans): PMTiles + MapLibre slider — triage view.
2. **SQL / agents**: GeoParquet drop of `field_ndvi ⋈ fields` + DuckDB. Optional
   showpiece: **"Ask the Fields"** — reuse the existing RunPod Qwen NL→SpatialSQL agent
   from `/Users/ross/ais-port-dwell/demo/` (same harness: local LLM writes DuckDB spatial
   SQL, DB verifies, table answers). "Which townships lost the most corn NDVI?" answered
   in English. ~Half a block of work since the harness exists; optional, cut-safe.
3. **ML handoff** (documented, not built): `field_ndvi` is the feature table a
   damage-classification or yield model trains on — schema and a 10-line sklearn sketch
   in docs, plus the honest line that model-building is a different repo (and where
   WherobotsAI raster inference would slot in commercially).

### Data-quality gates + pipeline run health (user-upgraded: real Great Expectations)
Two layers, per Ross's request:
- **Great Expectations (GX Core 1.x)** as the validation framework: expectation suites
  for `scenes` and `field_ndvi` — scene count per (season, tile) minimum (**catches the
  c1-l2a 2022 gap by construction**), NDVI ∈ [-1, 1], valid_frac distribution, no
  duplicate (field_id, date), field count vs source, row-count deltas vs prior snapshot
  (Iceberg time travel). Run as a checkpoint in `src/05_dq.py`; hard failures stop
  `make pipeline`. **GX Data Docs (HTML) published to GitHub Pages under /dq/** — a live
  DQ report is a strong SA artifact. Results also land in `local.crop.dq_results`.
  Build-time verify: GX Spark-engine compatibility with our pinned stack; fallback is GX
  over the gold GeoParquet via DuckDB/pandas (small tables, same suites) — checks stay
  identical either way.
- **Run health**: `local.crop.run_metrics` (run_id, stage, scope, rows_in/out, wall_s,
  scenes_processed, partitions_written, retries, started/ended_at) captured per stage;
  one docs chart (stage wall-clock by scope tier) + a README health snapshot. This is
  the observability story interviewers probe ("how do you know the pipeline is healthy?").

## User-confirmed decisions (revised)

1. **Sequencing**: **this repo FIRST** (Ross's call — the strongest Spark+Sedona showcase;
   raster is the rarer skill and Wherobots' own frontier). Wherobots trial block lands
   mid-build after Block 2/3 (hard-stop 2026-08-19) — by then the NDVI workload exists as
   real code to port, which strengthens the comparison. ais-port-dwell follows (its
   research pack in /Users/ross/job-hunt/wherobots/ doesn't expire).
2. **Cloud tier**: **EKS + spark-operator distributed run is CORE, never cut** — the
   point is demonstrating Spark-on-K8s operations, not just a cost row. EC2 spot
   single-node stays as the cheap middle rung; EMR Serverless = priced-but-not-run row.
   Cut order if squeezed: EMR row detail → spot rung → never EKS.
3. **Name**: `s2-field-ndvi` at `/Users/ross/s2-field-ndvi/`.

## Verified constraints digest (from 3-agent research fan-out, live-checked 2026-08-07)

Full agent reports are in the planning conversation; this digest is self-sufficient.

### Stack (pin exactly)
- JDK 17 Temurin (JDK 11 fallback if `InaccessibleObjectException` — upstream Sedona CI
  pairs Spark 3.5 with 11), Python 3.11, `pyspark==3.5.x`.
- `org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.1` — **MUST be 1.9.1, not 1.9.0**
  (1.9.0 has >180m ST_Transform regression, GH-3161 — silent corruption of every Iowa
  reprojection).
- `org.datasyslab:geotools-wrapper:1.9.1-33.5` — REQUIRED for raster (RasterUDT is
  GeoTools GridCoverage2D; missing jar = NoClassDefFoundError at first raster call).
- `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0`. Geometry → Iceberg is
  WKB-only (`ST_AsBinary`), same as AIS repo.
- Docker base: `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` (multi-arch, works on
  M4 + kind). Bake the two jars + hadoop-aws 3.3.4 into the image; never `--packages` at
  job start. `USER 185`. apache/spark-py and bitnami/spark are dead — do not use.
- S3 anon: `spark.hadoop.fs.s3a.aws.credentials.provider=...AnonymousAWSCredentialsProvider`,
  `fs.s3a.experimental.input.fadvise=random`.

### Data (all free, anonymous, public domain, verified live)
- **Sentinel-2**: Earth Search STAC v1 `https://earth-search.aws.element84.com/v1`,
  collection **`sentinel-2-c1-l2a`**. Asset keys are **`red`/`nir`/`scl`** (NOT B04/B08 —
  those are filenames). Hrefs → bucket `e84-earth-search-sentinel-data` (us-west-2, free);
  **rewrite https:// hrefs to s3a://** (Hadoop HttpFileSystem cannot seek → GeoTIFF decode
  fails). red/nir ~190-210MB each; scl ~1.4MB on the 20m grid (5490² vs 10980²).
  Reflectance = `DN * scale + offset` with scale/offset read from `raster:bands` per asset
  (uniformly 0.0001 / -0.1 across c1) — **never hardcode; NDVI is NOT offset-invariant**
  (+1000 DN makes real 0.85 read as 0.57 — fake stress).
- **TRAP: c1-l2a is missing essentially all of 2022 globally** (21 vs 1460 Iowa summer
  items). Decision: **skip 2022, don't backfill** — time axis = 2020 (event year) + 2023,
  2024, 2025, 2026-partial. Legacy `sentinel-2-l2a` has duplicate items per tile+date with
  mixed BOA conventions — avoid; if ever backfilling, keep only `_1_` baseline-05.00 items.
- Iowa = **29 MGRS tiles**, UTM zones 14T/15T/16T (EPSG:32614/15/16); 23 tiles in 15T
  (~88% of state); **6 tiles (15TWH 15TVH 15TVG 15TWG 15TVF 15TWF) = ~49% of state** (mvp
  scope). Filter `eo:cloud_cover` AND `s2:nodata_pixel_percentage` (partial swaths hit 61%
  nodata). Effective revisit 2026 ≈ 2.2 days (S2A+B+C); ~11 of 28 summer dates usable.
  Iowa coverage effectively starts 2018, not 2015.
- **Fields**: USDA Crop Sequence Boundaries 2018-2025 (released 2026-03-27):
  `https://www.nass.usda.gov/Research_and_Science/Crop-Sequence-Boundaries/datasets/NationalCSB_2018-2025_rev23.zip`
  (3.5GB ESRI GDB, national-only, EPSG:5070, public domain). Iowa filter:
  `CSBID LIKE '19%'` → ~631K+ polygons. Carries `CDL2018..CDL2025` crop-class columns
  (1=Corn, 5=Soybeans; corn+soy ≈83% of Iowa) → **no separate CDL download needed**.
  CSB is modeled from 30m CDL → buffer polygons inward -15m before zonal stats; drop
  fields <1ha. (fiboa GeoParquet on source.coop exists but is 2016-2023 + dissolved —
  cite, don't use. CropScape GetCDLFile endpoint is dead — timeouts.)
- **Boundaries**: TIGER2025 `tl_2025_us_state.zip`, `cb_2025_us_county_500k.zip` (11MB,
  web map); TIGERweb REST for quick GeoJSON.
- **Context raster layers (same Earth Search STAC — zero new plumbing)**:
  `cop-dem-glo-30` (Copernicus DEM 30m) → one-time per-field mean elevation + slope
  (CORE: cheap static zonal stats; map tooltip context + terrain control variable in the
  DiD). `landsat-c2-l2` → surface-temperature band as a per-field heat-stress column,
  demo county + 2025 season only (OPTIONAL, cut-safe; multi-sensor fusion beat that
  echoes the Landsat-ST samples in Wherobots' Data Hub). Sentinel-1 SAR stays future-work.
- **Deep-history tier (OPTIONAL): Landsat NDVI back to 1985.** Verified live 2026-08-07:
  `landsat-c2-l2` in the same STAC API returns central-Iowa scenes from 1985 (Landsat 5,
  `red`/`nir08` assets, full acquisition datetimes; 16-31 scenes/season in the 80s-90s,
  70+/season modern). Enables a 40-year per-field NDVI strip for the demo county — a
  differentiator no tutorial has. CAVEATS (verify at build): hrefs point at
  `s3://usgs-landsat` which is **requester-pays** (anonymous access fails; needs creds +
  requester-pays S3A support — check hadoop-aws 3.3.4 vs the 3.3.5+ requester-pays
  feature, else use the Planetary Computer mirror); 30m pixels; Landsat-vs-Sentinel NDVI
  are NOT directly comparable (sensor/bandpass differences — present as separate panels,
  never one spliced series; HLS harmonization is the production answer, cite don't build).
- **Derecho validation layer**:
  `https://www.nass.usda.gov/Research_and_Science/Disaster-Analysis/2020/Iowa_Derecho/Derecho_Iowa_082520_WindGust_Polygons.zip`
  (30KB shapefile, classes 60-79/80-99/100+ mph, public domain). Verified imagery pair on
  15TVG: **2020-08-04 (15.5% cloud) pre / 2020-08-19 (0.5% cloud) post**. Confound:
  concurrent drought (Iowa D1 34.3%→60.9% across the window) → difference-in-differences
  with wind polygons as treatment, matched-latitude controls (±0.15°). Optical NDVI
  understates lodging (flattened corn stays green; SAR outperformed NDVI in literature) —
  state as limitation; Sentinel-1 is future-work, not scope. Citations: NASA EO 147154,
  Remote Sensing 12(23):3878, BAMS 103(4).

### Temporal coverage & alignment (every source, one spine)

The common time spine is **(season, dekad)** — 10-day bins derived from each asset's
acquisition datetime. Every GeoTIFF's timestamp comes from its STAC item (`datetime`,
stored per-scene in `local.crop.scenes`); bands are joined **by scene_id**, so red/nir/scl
always come from the SAME acquisition — cross-date stitching is impossible by
construction, and a DQ check asserts one scene_id per (tile, band-triple) join anyway.
Static/annual sources join on coarser keys, stated explicitly:

| Source | Coverage (Iowa, verified) | Native cadence | Joins on |
|---|---|---|---|
| sentinel-2-c1-l2a | 2018→present, **2022 missing** | ~2-5 days (2.2d by 2026) | (scene_id) → (season, dekad) |
| sentinel-2-l2a legacy | 2017→present (dupes, mixed BOA) | same | 2022 backfill only if ever needed |
| landsat-c2-l2 | **1985→present** | 16d/sat (8d combined modern) | (season, dekad); separate panel, never spliced with S2 |
| cop-dem-glo-30 | static (one acquisition epoch) | n/a | field_id (one-time zonal) |
| CSB fields | boundary model vintage **2018-2025**; CDL labels 2018-2025 | annual labels | field_id; crop label by season |
| Wind-gust polygons | 2020-08-10 event only | n/a | spatial join, season=2020 |
| USDM drought | 2000→present, weekly | weekly | (season, week) — context/confound only |
| TIGER boundaries | annual vintages (2025 used) | annual | static |

Honesty constraint that falls out of this table: **CSB boundaries are a 2018-2025 model**
— applying them to 1985-2017 imagery assumes field geometry stability that weakens with
age. The deep-history tier reports this as a stated approximation (county-level rollups
are robust; per-field strips are labeled "modern boundaries applied retroactively").
Sentinel-2 before 2018 does not exist over Iowa in any collection (0 items 2015-16, 2 in
2017 — satellite-era fact, not a gap we can research away).

**Decision: no further blocking research.** Every load-bearing claim above is live-
verified; the only new unknown (requester-pays S3A for optional Landsat) is a 15-minute
build-time check on an optional tier. Build starts on verified ground.

### Sedona raster patterns (verified in source at tag sedona-1.9.1)
- Load with `spark.read.format("raster").option("tileWidth","256").option("tileHeight","256")`
  — streams via s3a with HTTP range reads, tiles during read, memory-safe (schema:
  `rast, x, y, name`). NOT binaryFile+RS_FromGeoTiff (whole-file byte[]). No out-db
  rasters in OSS (`RS_FromPath` = Wherobots/SedonaDB only — README gap-analysis material).
- STAC reader: `format("stac")` OSS since 1.7.1, live APIs, bbox+datetime pushdown;
  `eo:cloud_cover` typed but NOT pushed down; set `itemsLimitPerRequest=500`. Persist the
  scene manifest to Iceberg (reproducibility contract — STAC is mutable).
- **RS_MapAlgebra is DEPRECATED in 1.9.1** → use Python Raster UDFs (`with_bands()` +
  `nodata=`, both new in 1.9.1; use `as_numpy_masked()`, never `as_numpy()`). ONE UDF
  takes red+nir+scl columns: SCL cloud mask + BOA offset/scale + NDVI + nodata, one pass.
- SCL is 20m: read SCL with tileWidth/Height **128** so tile (x,y) indices align with the
  256px 10m tiles; join on (scene_id, x, y); `RS_ReprojectMatch(scl, red,
  'NearestNeighbor')` before combining (RS_AddBand throws on shape mismatch).
- **RS_ZonalStatsAll exactly ONCE per (tile, polygon)** — never
  SUM(RS_ZonalStats('sum'))/SUM(RS_ZonalStats('count')) (docs' own pattern runs the
  O(tileW×tileH)-per-polygon path twice — open perf issue GH-2409). RS_Clip is NOT a
  cheap prefilter (allocates full W×H). 256px tiles ≈ 2MB per call vs ~40MB at 1024px —
  this knob is hours-vs-days. Roll up SUM(s.sum)/SUM(s.count) GROUP BY field_id, date;
  carry valid_px/total_px.
- CRS: transform POLYGONS per UTM zone (`ST_Transform` + **explicit `ST_SetSRID`** —
  SRID-0 triggers silent double-transform garbage), broadcast them. `RS_Intersects` is an
  approximate WGS84 envelope check — candidate filter only; `lenient=true` handles false
  positives. No RS_Transform-to-EPSG exists.
- Never write raster columns to Iceberg — only reduced statistics.

### Compute + cost (verified pricing us-west-2)
- Laptop 16GB M4, **~44GB free disk — disk is a real constraint, solved by streaming**:
  raster reads are s3a HTTP range reads through Spark memory — COGs are NEVER stored on
  disk. What does hit disk: Iceberg warehouse (demo <1GB), PMTiles, Docker image (~2GB),
  shuffle spill (cap via spark.local.dir on a monitored path). The one big download,
  CSB's 3.5GB national GDB, is eliminated from the laptop path: pre-extract Iowa ONCE
  (single cloud/one-time job), publish `iowa_fields.parquet` (~300MB, 631K polygons) as a
  **GitHub Release asset** (2GB/file limit — fine); `make data` pulls that. The
  full-CSB-from-source path stays documented in the cloud runbook for from-scratch
  reproducibility. Laptop disk budget: ~5GB total. Demo tier egress ~0.8GB, <10 min.
- kind + `spark-submit --master k8s://` (native, no operator) + kubeflow spark-operator
  v2.5.2 SparkApplication as appendix. Docker Desktop VM at 10GB; `kind load docker-image`
  or eternal ImagePullBackOff; webhook must be Ready before applying SparkApplication.
- EC2 spot: r6i.4xlarge ~$0.45/hr (r7i.4xlarge seen ~$0.137/hr — check at launch).
  Full-state ≈ $2-4. **NAT gateway trap: $0.045/GB ≈ $10 > compute — public subnet or
  free S3 Gateway VPC Endpoint.** Write per-(date,mgrs_tile) Iceberg partitions so spot
  reclaim resumes.
- EKS: $0.10/hr control plane + 3× m6i.4xlarge spot ≈ $4-15 for the run; 6-12h first-time
  setup; `eksctl delete cluster` goes in the README (control plane bills while idle).
- EMR Serverless: $0.052624/vCPU-hr + $0.0057785/GB-hr; 64vCPU/256GB × 2h ≈ $9.69.
  Priced-but-not-run row.
- CI/CD, two tiers: (a) GitHub Actions free runner for the demo-tier smoke test only
  (3-6 min; the green badge reviewers expect; cache ~/.ivy2; 14GB disk is the limit —
  which is exactly why heavy runs don't belong there); (b) **Actions → OIDC → ECS Fargate
  dispatch**: a `workflow_dispatch` job assumes an AWS role via GitHub's OIDC provider
  (zero stored secrets) and launches the containerized mvp run as a one-shot Fargate task
  (max 16 vCPU/120GB, ~$0.04/vCPU-hr + $0.004/GB-hr → mvp run ~$1-2). This is the
  "production-shaped CI" story: no self-hosted runners, no long-lived keys, compute where
  the data is. EKS/EC2 remain the interactive scale-out paths; Fargate is the automated one.

### Web visual (verified)
- tippecanoe (`-o fields.pmtiles -Z6 -z12 -l fields`, drop-densest at low zooms only) →
  ONE tileset <100MB committed (GitHub hard block 100MiB/file; make_tiles.sh fails build
  at 90MB) + tiny county tileset for z<6. NOT ogr2ogr (Protomaps: worse overview tiles,
  no cross-tile stitching at this scale).
- Time dimension: WIDE props `d0..dN` quantized uint8 (NDVI clamped [-0.2,1.0] → 0-200;
  255 = masked when valid_frac<0.5 → grey on map). `fid` int32, not 20-char CSBID (~12MB
  of tile bytes). Slider = `setPaintProperty`. NOT per-date tilesets, NOT setFeatureState.
- MapLibre: **v6 is ESM-only (no UMD)** — pin 5.9.0 UMD + `pmtiles@4.4.1/dist/pmtiles.js`
  (explicit /dist path; package.json hides it). No build step, no npm. deck.gl explicitly
  rejected (flat choropleth needs no second tile pipeline) — README material.
- Hero: matplotlib+rasterio GIF (one tile, RdYlGn, fixed vmin/vmax, <5MB) + derecho
  before/after PNG pair. Precedents: OvertureMaps/explore-site (Pages workflow),
  opengeos/GeoLibre (ship GeoParquet alongside tiles), Simon Willison PMTiles TIL.
- **Architecture diagram as a first-class visual**: mermaid source in README (renders on
  GitHub) + an exported polished SVG/PNG in docs/img/ (mermaid-cli or excalidraw pass),
  linked from the Pages site footer. Shows the full flow: STAC → scenes → raster tiles →
  NDVI UDF → zonal → field_ndvi → DQ gate → {PMTiles map, GeoParquet/DuckDB + NL→SQL
  agent, ML handoff}, with the three compute targets (laptop/kind/EKS-Fargate) beneath.

## Repo structure

```
/Users/ross/s2-field-ndvi/
├── README.md                       # the product (outline below)
├── Makefile                        # setup data pipeline web hero demo
├── config.yml                      # EVERY threshold, tile lists, date windows, scope tiers
├── requirements.txt
├── docker/Dockerfile               # apache/spark:3.5.9... + sedona/geotools/hadoop-aws jars
├── docker/spark-defaults.conf      # pins + s3a anon + fadvise=random
├── src/config.py                   # sole reader of config.yml; scope tier → tiles/dates
├── src/session.py                  # SparkSession builder (local | k8s | cloud)
├── src/ndvi_udf.py                 # THE raster UDF: SCL mask + offset + NDVI + nodata
├── src/01_fields.py                # CSB GDB + wind polygons + counties → Iceberg
├── src/02_scenes.py                # STAC → Iceberg scene manifest
├── src/03_ndvi_zonal.py            # raster read → UDF → RS_ZonalStatsAll → field_ndvi
├── src/04_publish.py               # wide pivot → GeoJSONSeq + GeoParquet drop
├── src/05_dq.py                    # GX checkpoint → dq_results + Data Docs; fails pipeline
├── src/06_context.py               # DEM slope/elev per field (core) + Landsat ST (optional)
├── scripts/fetch_data.sh           # iowa_fields.parquet (Release asset), TIGER, wind polys
├── scripts/extract_csb.py          # one-time: 3.5GB national GDB → Iowa parquet (cloud runbook)
├── scripts/make_tiles.sh           # tippecanoe; FAILS at >90MB
├── scripts/make_hero.py            # GIF + derecho before/after PNGs
├── notebooks/derecho_event_study.ipynb   # the ONE notebook: DiD, wind-class table
├── web/index.html + app.js         # MapLibre 5.9.0 UMD + pmtiles, no build step
├── web/fields.pmtiles + counties.pmtiles
├── wherobots/run_zonal.py          # trial chapter; NOT in Makefile
├── k8s/kind-cluster.yml + sparkapplication.yml
├── docs/architecture.md            # mermaid + data contracts
├── docs/spark-notes.md             # Spark engineering notes (see below)
├── docs/wherobots-trial.md         # measured comparison + OSS-vs-platform gap analysis
├── docs/img/
├── demo/ask_fields/                # OPTIONAL: NL→SQL agent over the GeoParquet drop
│                                   # (port of /Users/ross/ais-port-dwell/demo/ harness)
├── .github/workflows/smoke.yml     # demo-tier pipeline on free runner
├── .github/workflows/mvp-fargate.yml  # workflow_dispatch → OIDC → ECS Fargate one-shot task
├── .github/workflows/refresh.yml   # weekly cron: incremental county update + Pages redeploy
├── .github/workflows/pages.yml
└── .gitignore                      # data/ warehouse/ *.tif *.gdb *.zip
```

## Pipeline stages + Iceberg contracts

Scope tiers (one `--scope` flag, resolved by src/config.py):

| tier | extent | time axis | egress | runs on | produces |
|---|---|---|---|---|---|
| `demo` (default) | 1 county on 15TVG | 2020-08-04 + 2020-08-19 | ~0.8GB | laptop <10 min | derecho headline table |
| `mvp` | 6 tiles (49% of state) | 2025 season 15 dekads + 2020 pair | ~35GB | laptop overnight / 1 spot-hr | the committed PMTiles |
| `state` | 29 tiles | 2020 + 2023-2026 seasons | ~230GB | EC2 spot + EKS us-west-2 | cost-table numbers |
| `history` (planned scale-out) | 29 tiles + Landsat | S2 all 8 seasons (2018-21, 23-26) + Landsat 1985-2017 county strip | ~1.7-2TB | EKS 6-10 nodes, ~$20-40 | climatology + event library |

Demo county = the county fully inside 15TVG with largest 100+ mph wind-polygon overlap
(picked by one query in Block 1, then pinned in config.yml). Time unit = 10-day dekads,
growing season only (DOY 121-273), best scene per (tile, dekad) by cloud cover.

### Maximal-history feasibility & capacity model (the "can we, and what does it cost" math)

Volume at maximal scope, from verified per-asset sizes:
- S2: 29 tiles × ~15 dekads × 8 seasons ≈ 3,500 scene-selections × ~0.4GB (red+nir+scl)
  ≈ **1.4TB scanned** (streamed, never stored). Zonal calls ≈ 631K fields × ~120 dates ×
  ~2 tile-overlaps ≈ **~150M RS_ZonalStatsAll calls** at ~2MB transient each (256px
  tiles) — fine distributed, absurd on a laptop.
- Landsat 1985-2017: ~9 WRS-2 footprints × ~5 usable/season × 33 seasons ≈ 1,500 scenes
  × ~0.2GB ≈ **+300GB** (county-scope strip cuts this ~20×).
- Outputs stay tiny: ~76M field_ndvi rows ≈ **~4GB Parquet** — the lakehouse output is
  laptop-holdable even when the input is 2TB. That asymmetry IS the lakehouse pitch.

Cost/time, derived from the verified baseline (full-state single season ≈ 230GB ≈ 2-4h
on 16 vCPU ≈ $2-4 spot): workload is embarrassingly parallel over scene_id (broadcast
polygons, no raster shuffle), so wall-clock ≈ bytes/cores and cost ≈ bytes, not nodes:
- 16 vCPU spot: full history ≈ 15-30h — too long, don't.
- **EKS 6-10 spot nodes (96-160 vCPU): ~3-6h, ~$20-40 all-in.** EMR Serverless
  equivalent ~$40-80. Verdict: **the maximal scope is under $50 — affordable by design.**

**The capacity model is itself a deliverable** (`docs/spark-notes.md` + README): Block 1
measures per_scene_cost at demo tier → `wall_clock ≈ scenes × per_scene_cost /
executor_cores` predicts every bigger tier BEFORE running → each cloud run records
predicted-vs-actual (target ±30%). An SA who shows a capacity model that called its own
runtime is demonstrating the exact skill the role sells. Scale-out rule: grow executors,
never redesign — partitions (date, mgrs_tile) are the work AND restart units, so
expansion = same code, bigger node group, `eksctl delete` after.

### Scheduling & incremental operations (live-source cadence, catch-up by design)

**Core principle (already in the architecture): the pipeline is incremental and
idempotent, so the scheduler is interchangeable and catch-up is free.** Each run:
re-query STAC for a trailing window → diff against the `scenes` manifest and existing
`field_ndvi` partitions → process only the delta → append/overwrite those partitions.
If the scheduler is off for N weeks, the next run picks up N weeks of drops with zero
special-case code. Sedona is the engine inside a run; scheduling lives outside it.

- **Late-arriving data (the classic gotcha, handled)**: STAC items can land days after
  acquisition, and a later scene can beat an earlier one for best-per-dekad. Policy: each
  run re-resolves selection for a **trailing 30-day window** and dynamic-partition-
  overwrites those (date, mgrs_tile) partitions; older partitions are immutable. Iceberg
  snapshots make every refresh auditable (and revertible).
- **Implemented scheduler: GitHub Actions cron** (`.github/workflows/refresh.yml`,
  weekly): runs the demo-county incremental update on the free runner, refreshes the
  2026 "current season" panel + PMTiles, redeploys Pages. Free, versioned with the repo,
  visibly alive ("last updated" stamp on the map = credibility). Well within runner
  limits since deltas are ~1-2GB at county scope.
- **Documented (not left running): AWS-native cadence** — EventBridge Scheduler cron →
  the same ECS Fargate task definition the OIDC workflow uses → state-scale weekly
  refresh for ~$0.50/run. Ships as config + README recipe, default OFF (a portfolio repo
  should not surprise-bill). Airflow/Dagster get one evaluated-and-rejected paragraph:
  the enterprise answer, pointless to babysit for one DAG — and this repo's idempotency
  is exactly what makes ANY of those orchestrators easy to drop in later.
- **Per-source cadence plan** (extends the temporal table): S2 c1-l2a = continuous
  (~daily new scenes; trailing-window logic absorbs it); landsat-c2-l2 = continuous;
  USDM = weekly (tiny CSV fetch); CSB = annual (~March; documented "vintage bump"
  procedure: update config, re-run 01_fields, labels-only reprocess); CDL = annual (Feb,
  unused directly); TIGER = annual; DEM = static.
- **README demonstration**: a log excerpt of a catch-up run after a deliberate 3-week
  gap ("scheduler off → on: 6 dekad-partitions detected missing, 6 processed, DQ green")
  — proof the semantics work, not just a claim.

### Why more history makes the downstream layer better (it does — materially)
1. **Climatology anomaly layer** — the analytic upgrade: with 8 S2 seasons, every
   (field, dekad) gets its own median/IQR baseline → the map and SQL layer serve
   **z-scores vs this field's own history**, not raw NDVI. "Stressed" becomes defensible.
   Impossible with 1-2 seasons; this is the single biggest payoff of full history.
2. **Event library for ML**: 2020 derecho (wind-labeled) + 2024 NW-Iowa floods + 2023/25
   drought weeks (USDM-labeled) → a multi-event labeled training set (field-dekad,
   anomaly, event-type) — the ML handoff stops being hypothetical; docs/ml-handoff gains
   a real supervised-learning sketch.
3. **Landsat 40-year strip**: long-run trend/land-use context at county rollup (robust to
   CSB boundary vintage); lets the NL→SQL agent answer "compare 2020 to the 2012 drought"
   — questions raw single-season data cannot touch.
4. **DQ gets sharper**: climatology bounds catch statistically impossible NDVI jumps that
   static range checks miss.

- **S1 `local.crop.fields`**: field_id(CSBID), county_fips, utm_zone, area_m2,
  cdl2020/23/24/25, geom_4326/geom_utm/geom_utm_buf (all WKB) — partitioned by utm_zone.
  Plus tiny `wind_zones` (3 rows) and `counties` tables. `06_context.py` appends
  elev_m + slope_deg per field (cop-dem-glo-30 zonal, one-time) and, optionally,
  lst_mean per (field, dekad) from landsat-c2-l2 ST (demo county, 2025 only).
- **S2 `local.crop.scenes`**: scene_id, mgrs_tile, date, dekad, season, cloud_cover,
  nodata_pct, epsg, red/nir/scl_href (s3a-rewritten), red/nir_offset+scale (read from
  STAC, never hardcoded), selected, ingested_at — partitioned by (season, mgrs_tile).
- **S3 `local.crop.field_ndvi`**: field_id, date, dekad, season, mgrs_tile, utm_zone,
  scene_id, mean_ndvi, valid_px, total_px, valid_frac — **partitioned by (date, mgrs_tile)
  = the restart unit**. Driver diffs selected scenes vs existing partitions → append-only,
  idempotent, resumable (spot-reclaim-safe).
- **S4**: field_ndvi ⋈ fields → GeoJSONSeq → tippecanoe → PMTiles; plus a GeoParquet drop
  of results (queryable, GeoLibre precedent — this is also the NL→SQL agent's substrate).
  Tiles are a view; the lakehouse is truth (that's what keeps tiles <100MB — only 2025
  season + 2020 event in tiles).
- **S5 `local.crop.dq_results`**: check_name, scope, expected, observed, pass, run_at.
  Runs after S2 (scene checks incl. the 2022-gap per-season minimum) and after S3 (NDVI
  range, valid_frac, dup keys, count deltas via time travel). Hard failures stop `make
  pipeline`; the table itself is a README exhibit.
- **S1 fetch note**: laptop path pulls the pre-extracted `iowa_fields.parquet` Release
  asset (~300MB); `scripts/extract_csb.py` documents the one-time 3.5GB GDB → parquet
  extraction (run in cloud or once locally then deleted).

Derecho event study = same field_ndvi table, `WHERE season=2020` + wind_zones join, ~20
lines of DiD SQL in the notebook. Claim is MONOTONICITY of Δ(treated)−Δ(control) across
wind classes, not absolute drop.

### What Iceberg honestly earns here (schema evolution belongs to the AIS repo)
1. Restartable incremental append (partition = unit of work; weekly 2026 appends same path).
2. Scene manifest as reproducibility boundary (STAC is live/mutable; the snapshot is not).
3. **Partition evolution, for real**: demo writes PARTITIONED BY (date); scaling to state
   runs `ALTER TABLE ... ADD PARTITION FIELD mgrs_tile` — old data queryable, zero
   rewrite; show `.partitions` metadata before/after.
4. Time travel as the sensitivity engine: retune scl_mask_classes, diff VERSION AS OF vs
   current → the README sensitivity numbers come from metadata.
Say what it does NOT earn: no schema-evolution story (link ais-port-dwell), no row-level
deletes, no compaction section.

## Spark-experience signal (user-requested emphasis)

The critical path is 100% OSS Spark; make the engineering visible:
- `docs/spark-notes.md` + README section: partitioning strategy, why broadcast (631K
  buffered polygons ≈ low hundreds of MB → map-side join, no raster shuffle), AQE, shuffle
  partitions, executor sizing math for the spot box and EKS pods.
- **Measured scaling table**: same mvp job at local[4] / local[10] / 16-vCPU spot /
  3-node EKS — wall-clock + $ per row. Spark UI screenshot of the zonal stage in
  docs/img/ (DAG + task distribution).
- The 256px-tile decision documented as a measured tradeoff (per-call allocation vs task
  count), with the GH-2409 issue cited — engine-internals literacy, not cargo-culting.
- Three run targets from one image: `make pipeline` (local), `spark-submit --master k8s`
  (kind), same manifest on EKS. Operator appendix shows the declarative variant.

## Wherobots trial block (ONE 4h block, serves BOTH repos, hard-stop 2026-08-19)

Register/verify the account **today** (day-0 auth check; discovering an auth problem on
day 10 is the failure mode). Runs after Block 2/3 of THIS repo, so the NDVI workload
already exists as real code to port. Priority order, stop when time is up:
1. NDVI zonal (demo-county scope) ported to their runtime — measured row for the
   comparison table; the interesting deliverable is the DIFF: what had to change vs OSS
   (should be session/catalog/paths only; if more, that IS the finding). (~75 min)
2. AIS sample workload on their platform (their own vertical) — screenshots + timing
   banked for ais-port-dwell's later README. (~60 min)
3. Out-db raster demo: `RS_FromPath` on the identical COG hrefs — the concrete thing OSS
   structurally lacks. One screenshot, five lines. (~45 min)
4. CSB vs `fields_of_the_world_vector_global` on the demo county (their Data Hub AI
   boundaries): count/area/overlap, one figure. **Cut first.** (~30 min)
Skip explicitly: RS_CLASSIFY raster inference (different repo; saying so > half-demo).
**Gate**: if their runtime isn't executing your code 90 min in, stop → paper gap analysis
from published docs + already-verified source facts (45 min, prewritten structure in
docs/wherobots-trial.md). A clean paper analysis beats a half-authenticated screenshot.

## Build schedule (THIS repo first; trial block after Block 2/3, before 2026-08-19)

- **Block 1 — "one NDVI number I believe"**: fetch data; 01_fields (demo county);
  02_scenes (derecho pair); spike 03 end-to-end. EXIT GATE: field_ndvi holds 2 dates ×
  ~20K fields; corn mean NDVI on 2020-08-04 ∈ [0.6, 0.9]. If behind, cut: 128px SCL trick
  — broadcast whole SCL per scene (1.4MB) and move on.
- **Block 2 — "hero + map"**: make_hero.py; DiD SQL + wind-class table; 04_publish +
  tippecanoe + web/; architecture diagram (mermaid + SVG export). EXIT GATE: Pages serves
  working county map with slider; hero.gif <5MB; wind-class table exists WHATEVER it
  says; diagram in docs/img/. If behind, cut: county tileset (set minzoom 8 + "zoom in"
  overlay), SVG export (mermaid-in-README only).
- **Block 3 — "mvp scale"**: kick 6-tile mvp run first (background grind), regenerate
  PMTiles <100MB, kind + spark-submit run for the K8s story. EXIT GATE: mvp tiles
  committed; kind run documented. If behind, cut: the mvp run — ship 2-tile scope, mark
  state row "estimated," never present modeled numbers as measured.
- **Block 4 — "EC2 + EKS runs" (EKS is the centerpiece, never cut)**: eksctl cluster +
  spark-operator + same image/manifest for the state-scale distributed run (us-west-2,
  public subnet or S3 gateway endpoint); EC2 spot single-node as the middle scaling rung;
  measured scaling table + Spark UI screenshots; `eksctl delete cluster` same day.
  EXIT GATE: EKS run measured; cost table rows filled; cluster deleted. If behind, cut:
  the spot middle rung (EKS + laptop rows still tell the scaling story), then Landsat ST.
- **Block 5 — "the README is the product"**: all sections (purpose one-liner up top),
  thresholds+sensitivity (via time travel), limitations, evaluated-and-rejected,
  spark-notes, DQ exhibit, smoke.yml + pages.yml + refresh.yml (weekly cron + one staged
  catch-up demonstration for the README log excerpt) + mvp-fargate.yml (OIDC role +
  task-definition; if the AWS side drags, ship the workflow with the role ARN as a
  documented placeholder and label the row "wired, not exercised"). EXIT GATE: cold clone
  → `make setup && make data && make demo` <15 min; CI green; **public-safety checklist
  passes (gitleaks clean, no ARNs/paths/PII, licenses cited) before the repo flips
  public**. If behind, cut: Fargate workflow before README sections; never cut README
  time or the safety checklist.
- **Block 6 (OPTIONAL) — "Ask the Fields"**: port the RunPod Qwen NL→SQL harness from
  /Users/ross/ais-port-dwell/demo/ onto the GeoParquet drop; 5 pinned example questions
  with verified answers; screenshot for README. Cut-safe: the repo is complete without
  it; do it only if Blocks 1-5 landed and the interview timeline allows.

## Top risks

1. **Derecho signal not monotonic** (drought confound + NDVI blind to lodging). Mitigate:
   build DiD BEFORE map/README; pre-register thresholds in config.yml; never tune the mask
   to prettify. A reported null result + SAR explanation is stronger SA signal than a
   tuned chart. Top risk because its failure mode is dishonesty.
2. **PMTiles >100MiB hard block at git push.** make_tiles.sh fails at 90MB; one season in
   tiles; uint8; int fid; measure at demo scope and extrapolate before mvp.
3. **SCL/red tile misalignment** → empty join that "ran fine." Assert per-scene tile
   counts match; validate one tile's masked pixel count against rasterio ground truth.
4. **Zonal cost explosion** (GH-2409). ZonalStatsAll-once + broadcast + no RS_Clip;
   measure per-scene wall clock at demo scope, extrapolate before committing to a tier.
5. **Egress blowout on laptop** — `make pipeline` prints estimated GB + scene count before
   reading anything; state tier only in us-west-2.
6. **CRS silent garbage** (SRID-0 double transform). Explicit ST_SetSRID everywhere;
   assert transformed centroids inside scene envelope; 50-polygon visual overlay check in
   the notebook.
7. **Trial burns the block** — register day 0; 90-min gate; prewritten paper fallback;
   launch mvp run first so the block produces something regardless.
8. **Version traps** — sedona 1.9.0 resolving instead of 1.9.1; wrong-Scala jar;
   hadoop-aws drift; JDK 17 module errors. make setup asserts exact versions and fails
   loudly; CI runs the pinned Docker image.

Killed scope (do not re-add): Sentinel-1 (future-work line), 2022 backfill, RS_CLASSIFY,
separate CDL download, deck.gl, ogr2ogr tiling, per-date tilesets, EMR Serverless run.

## README outline (Block 5 checklist)

1. Title + purpose one-liner + CI/Pages badges. 2. Business question ("which fields lost
yield capacity in the Aug 2020 derecho — answered for 631K fields without a GIS desktop")
+ who consumes it (map / SQL+agent / ML features).
3. Hero: GIF + before/after pair + wind-class table above the fold. 4. Live map link.
5. Quickstart: `make setup && make demo`, ~10 min, $0, no cloud account. 6. Architecture
(mermaid + contracts). 7. Data sources table (2022 gap called out). 8. Scope tiers +
measured cost table. 9. Thresholds + sensitivity. 10. What Iceberg earns here (and
doesn't — link ais-port-dwell). 11. Spark engineering notes + scaling table + UI
screenshot. 12. Wherobots Cloud comparison (measured or paper). 12b. Data-quality gates (the dq_results exhibit + the Soda/GE mapping + the 2022-gap
catch). 12c. Downstream consumption (GeoParquet/DuckDB quickstart, Ask-the-Fields if
built, ML-handoff schema sketch). 12d. Operations: incremental refresh + scheduling
(cron workflow, catch-up log excerpt, EventBridge/Fargate recipe, per-source cadence
table, Airflow/Dagster paragraph). 13. Limitations (NDVI vs
lodging, drought confound, CSB edges, cloud gaps, missing 2022, dekad compositing).
14. Evaluated-and-rejected (deck.gl, RS_MapAlgebra, out-db OSS gap, ogr2ogr, Databricks
free, fiboa-as-primary, per-date tilesets, setFeatureState, MapLibre v6 UMD, CDL download,
2022 backfill, Sentinel-1). 15. Reproduce at scale (spot recipe + NAT warning, kind/EKS,
EMR pricing). 16. Citations + licenses (code Apache-2.0, data public domain).

## Public-repo safety checklist (repo goes public — enforced at Block 5 gate)

- **Start private, flip public only after a clean scan**: run gitleaks (or trufflehog)
  over the FULL git history, enable GitHub secret scanning + push protection before the
  flip. If anything ever leaked into history, rewrite before publishing, not after.
- **No AWS identifiers in code or docs**: the OIDC role ARN contains the account ID —
  it lives in a GitHub Actions secret/variable; committed docs use
  `arn:aws:iam::<ACCOUNT_ID>:role/...` placeholders. No account IDs, no bucket names of
  private buckets, no VPC/subnet IDs (public-data buckets like sentinel-cogs are fine).
- **Wherobots trial artifacts scrubbed**: screenshots cropped/redacted of org name,
  account email, workspace IDs before landing in docs/img/.
- **Ask-the-Fields endpoint**: the RunPod Qwen URL/key is env-var only (.env in
  .gitignore, .env.example committed with placeholders) — never hardcoded.
- **No personal absolute paths in repo content**: repo docs/scripts use relative paths
  (the /Users/ross convention is for chat/vault outputs, not the public repo). Review
  generated artifacts (GX Data Docs HTML, notebook outputs) for embedded local paths and
  emails before commit.
- **Data/license hygiene**: commit only derived, public-domain-sourced artifacts (CSB/
  CDL/TIGER/NOAA/USDA = public domain, verified); cite USDA's own license text for the
  fields extract (fiboa's "proprietary" STAC field is a known mislabel); no Maxar or
  other NC-licensed data anywhere. LICENSE = Apache-2.0 for code; data attribution
  section in README.
- **.gitignore from day 0**: data/, warehouse/, .env, *.pem, *.key, spark-warehouse/,
  metastore_db/, .ipynb_checkpoints/ — plus a pre-commit size guard (blocks >50MB adds)
  so a stray GeoTIFF never enters history.
- **Commit identity**: use the GitHub noreply email if Ross prefers his personal email
  unpublished (his call at repo init).

## Verification

- Per-stage asserts: corn NDVI range check (Block 1 gate); SCL alignment tile-count
  assert; CRS centroid-in-envelope assert; rasterio ground-truth comparison for one tile;
  DiD notebook runs end-to-end from Iceberg only.
- `make demo` on a cold clone <15 min is the reproducibility bar; CI smoke.yml runs the
  demo tier on every push (cache ~/.ivy2); pages.yml deploys web/.
- Three gates before "done": builds, smoke passes, behavior = README claims (every number
  in the README traceable to a table or a labeled estimate).

## Implementation start

Per revised sequencing, implementation starts with THIS repo. Day-0 actions: (1) verify
Wherobots account login works (trial clock is running; the block itself comes after
Block 2/3), (2) scaffold /Users/ross/s2-field-ndvi/, copy this plan's digest into its
docs/ and to /Users/ross/job-hunt/wherobots/raster-build-plan.md, (3) Block 1.
ais-port-dwell (plan: /Users/ross/job-hunt/wherobots/build-plan.md) follows this repo.
Also at build start (vault hook, deferred — plan mode is read-only): log to
/Users/ross/vault/changelog.md (raster project planned, trial window, revised sequencing)
and add the digest as /Users/ross/vault/s2-field-ndvi-research-digest.md.
