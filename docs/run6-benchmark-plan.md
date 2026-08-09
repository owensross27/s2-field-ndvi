# Run 6: mvp-scope cloud benchmark — push-button plan

**Status: EXECUTED 2026-08-08/09. Results in `docs/spark-notes.md` ("Run 6"),
raw logs in `artifacts/run6/`. Total spend ~$2.**

Outcome in one paragraph: the **engine head-to-head landed** (jiffle 75 s/scene vs
python_udf 74 s/scene vs python_udf+tiling 114 s/scene, all 13,369 rows, 16 vCPU
in-region) and settles the upstream-deprecation question — python_udf is free to
adopt. The **mvp rows did not**: row 1 OOMed (a driver heap that was silently 1g,
see the `--driver-memory` warning below) and row 2 ran healthy but could not finish
a single scene in 100 minutes. That failure is itself the headline finding: this
plan's ~69 s/scene estimate modeled download time, while the real bottleneck is a
zonal join that is superlinear in **fields per scene**. A future mvp/state attempt
must batch by field count, not by scene — do not simply relaunch these rows as
written. Two 8xlarge spot instances were also reclaimed mid-run by AWS; the
successful arms ran on a small on-demand box.

The plan below is preserved as written (pre-registered), with per-row results
noted inline.

**Run-7 addendum (2026-08-09)**: the mvp wall was root-caused (Sedona
SpatialIndexExec's serial per-action broadcast index rebuild — see spark-notes
"Run 7") and fixed with a tile-grid equi-join, exact-signature-verified. mvp
scenes now complete at 954-1253s each on 16 Graviton vCPU; 5/41 partitions are
banked in `s3://s2fn-run6-384555717200/run7/wh-mvp.tar.gz`. Finishing the matrix
belongs on the AWS Batch array topology (this doc's successor plan): diversified
c7g/m7g .2xl-.4xl spot pools, one child per scene, idempotent appends, **Glue
Data Catalog instead of the Hadoop catalog for concurrent writers** (Hadoop
catalog commits require atomic rename, which S3 does not have — single-writer
only). Estimated ~$2.50 / ~2.5h for the remaining 36 scenes.

Companion docs, read first: `docs/spark-notes.md` ("Cloud-run findings", "Flag
economics", "Container validation"), `docs/k8s-runbook.md` rung (b) [EKS
translation] and rung (c) [benchmark matrix], `docs/build-plan.md` ("Compute +
cost"), `config.yml`, `Makefile`.

## 0. Why run 6, in one paragraph

Runs 4 and 5 both died on the same box shape (`/home/ec2-user/s2fn/...`, confirmed
from `/Users/ross/s2-field-ndvi/artifacts/run.log` and `run5.log`), at mvp scope
(`SCOPE=mvp`, 41 selected season scenes — confirmed in `run5.log`: "scenes: 41
selected, 0 partitions done, 41 to process"). Run 4 died of cross-scene shuffle
spill (~30GB, on a box with ~60GB disk). Run 5 died of `TaskResultLost` collecting
the statewide fields broadcast as one 838MB task result (confirmed in
`run5.log:67-259`). Both fixes now live behind `config.yml` flags
(`raster.per_scene`, and per-scene fields pruning that rides along with it) but
neither has been benchmarked past demo scope (`docs/spark-notes.md` "Flag
economics" — 1 tile, 2 scenes only). Run 6 is the first mvp-scope cloud run with
both fixes live, structured as a benchmark matrix so the numbers in
`docs/spark-notes.md` and `docs/k8s-runbook.md` stop being placeholders.

## 1. Instance choice

| Instance | vCPU | RAM | On-demand (us-west-2, list) | Notes |
|---|---|---|---|---|
| m6i.8xlarge | 32 | 128 GiB | ~$1.536/hr | primary choice |
| r6i.8xlarge | 32 | 256 GiB | ~$1.848/hr | memory headroom for python_udf big-heap retry |
| m6i.16xlarge | 64 | 256 GiB | ~$3.072/hr | 2x cores of m6i.8xlarge, 2x price |

On-demand list prices are AWS's published us-west-2 rate card at time of writing —
**verify at launch, prices drift**. Spot is typically 60-70% off list but is not
guaranteed capacity, which is exactly the failure mode below.

**Pick m6i.8xlarge, spot, with r6i.8xlarge as the on-launch fallback family.**

Reasoning:
- 32 vCPU keeps the matrix (section 4) inside a single afternoon without paying
  16xlarge's linear-in-price-not-linear-in-need premium — `docs/spark-notes.md`
  "Scaling economics" already established wall-clock ≈ work / cores, cost ≈ work,
  roughly flag-invariant of how cores are packaged. 8xlarge is the sweet spot
  between "fits the whole matrix in one box" and "not paying for cores the
  python_udf retry's single-partition-at-a-time recipe can't use anyway."
- **Spot capacity lesson**: on the night runs 4/5 were prepped, spot had ZERO
  m6i capacity in us-west-2 for the needed size. Do not `aws ec2 run-instances`
  blind and wait — check capacity first, and have the fallback family ready to
  swap into the same launch template:
  ```bash
  aws ec2 describe-spot-price-history \
    --instance-types m6i.8xlarge r6i.8xlarge \
    --region us-west-2 --product-descriptions "Linux/UNIX" \
    --start-time "$(date -u -v-1H +%FT%TZ)" --max-results 20
  ```
  If `m6i.8xlarge` shows no recent spot fills, go straight to `r6i.8xlarge` spot
  (more RAM is strictly useful for the python_udf retry in section 4 anyway).
- **On-demand ceiling**: if spot has zero capacity in both families, the fallback
  is on-demand m6i.8xlarge at the run budget's hard number: **do not exceed
  $2.00/hr on-demand** without asking Ross first — that's the whole run-6 budget
  (section 4) burned in under 3 hours even before disk/data transfer.
- 250GB gp3 root volume — run-4 lesson (died at ~30GB spill on a box that only
  had ~60GB total). 250GB gives headroom for the warehouse restore (section 3),
  shuffle spill even with `per_scene` capping it, and Spark event logs, at gp3's
  ~$0.08/GB-month (irrelevant at same-day teardown, a few cents).
- **us-west-2, no exceptions.** In-region S3 reads are the whole point — the
  Sentinel-2 bucket (`e84-earth-search-sentinel-data`) lives there, and
  `docs/spark-notes.md`'s capacity model explicitly separates "home broadband"
  numbers from the in-region prediction this run exists to measure.

```bash
aws ec2 run-instances \
  --region us-west-2 \
  --instance-type m6i.8xlarge \
  --instance-market-options '{"MarketType":"spot","SpotOptions":{"MaxPrice":"1.00"}}' \
  --image-id <amazon-linux-2023-ami-id> \
  --key-name <keypair> \
  --security-group-ids <sg-id> \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":250,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=s2fn-run6}]'
```

`--instance-market-options` is the documented spot form for `run-instances`
(distinct from a Spot Fleet request) — confirm the exact JSON shape against
`aws ec2 run-instances help` at launch time, this has not been executed from this
worktree.

## 2. Software: containerized path, recommended

Two options exist in this repo:

- **(a) Containerized**: `make image-amd64` (buildx + QEMU, cross-compiles from
  the M4 laptop to amd64) → push to ECR → `docker run` on the EC2 box. Same
  image `docs/k8s-runbook.md` rung (b) already documents for EKS.
- **(b) Native**: `git clone` + `make setup` directly on the EC2 box (this is
  what runs 4 and 5 did — `run.log`/`run5.log` show `.venv`, `uv`, and ivy jar
  resolution happening on-box under `/home/ec2-user/s2fn/`).

**Recommend (a), containerized**, with one caveat.

Reasoning: `docs/spark-notes.md` "Container validation" already measured
in-container jiffle at **189 s/scene, exact baseline signature** — parity with
native. The image removes setup drift (exact jar pins, exact Python 3.11, no ivy
resolution against Maven Central at job start racing rate limits) that runs 4/5's
native path was exposed to. The added ECR push step is a fixed ~5-10 min one-time
cost against a run this size; setup drift on a fresh box is the more expensive
failure mode to debug mid-run.

```bash
make image-amd64
aws ecr create-repository --repository-name s2-field-ndvi --region us-west-2
aws ecr get-login-password --region us-west-2 | \
  docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-west-2.amazonaws.com
docker tag s2-field-ndvi:latest <account-id>.dkr.ecr.us-west-2.amazonaws.com/s2-field-ndvi:latest
docker push <account-id>.dkr.ecr.us-west-2.amazonaws.com/s2-field-ndvi:latest
```

On the EC2 box:

```bash
aws ecr get-login-password --region us-west-2 | \
  docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-west-2.amazonaws.com
docker pull <account-id>.dkr.ecr.us-west-2.amazonaws.com/s2-field-ndvi:latest
docker tag <account-id>.dkr.ecr.us-west-2.amazonaws.com/s2-field-ndvi:latest s2-field-ndvi:latest
```

Caveat: the container path was only validated at demo scope on Docker Desktop
(10GB VM). This run is the first time it sees mvp scope on real Linux — keep the
native path (`git clone` + `make setup`, `Makefile`'s `setup` target) as a fallback
if the image hits something the demo-scope validation didn't exercise.

## 3. Warehouse restore decision

Salvage: `/Users/ross/s2-field-ndvi/artifacts/mvp-warehouse.tar.gz` (838MB — mvp
fields table 803M + scenes manifest). This is a **Hadoop-catalog Iceberg
warehouse**: table metadata embeds the ABSOLUTE creation path
(`/home/ec2-user/s2fn/warehouse/...`, confirmed from `run.log`/`run5.log`'s own
paths). `docs/spark-notes.md` "Never copy a Hadoop-catalog Iceberg warehouse":
copying it anywhere else yields tables that silently read from — and write into —
the original path.

**Two options:**

- **(a) Restore to the identical path.** Only works if the run-6 box's warehouse
  ends up at exactly `/home/ec2-user/s2fn/warehouse` (native path) — meaning the
  container path's `S2FN_WAREHOUSE` env override (see `src/session.py`,
  `os.environ.get("S2FN_WAREHOUSE")`) would need to be set to that exact absolute
  string, not `/opt/s2fn/warehouse`, or the container's own `REPO_ROOT`-relative
  default. Saves ~5 min of re-running `01_fields.py` + `02_scenes.py`.
  ```bash
  mkdir -p /home/ec2-user/s2fn/warehouse
  tar xzf mvp-warehouse.tar.gz -C /home/ec2-user/s2fn/warehouse --strip-components=1  # verify strip depth against the actual tarball layout before running
  ```
- **(b) Rerun `01_fields.py` + `02_scenes.py` fresh.** No path coupling, no risk
  of a container mount silently landing the extracted tables at the wrong
  absolute path and producing a `FileNotFoundException` mid-`03_ndvi_zonal.py`
  (the exact class of trap `docs/k8s-runbook.md` step 2 documents for the kind
  case). Costs the ~5 min the restore would have saved.

**Recommend (b) unless the launched instance's user-data / container mount is
verified, before the matrix starts, to land the warehouse at the identical
absolute path.** The failure mode of getting (a) wrong is silent (a
`FileNotFoundException` deep into stage 35+ of `03_ndvi_zonal.py`, the same stage
number run 5 died at) or, worse per the "Never copy" note, a write that routes
back into a path that doesn't exist on this box at all. 5 minutes is cheap
insurance; a wrong-path failure discovered an hour into the matrix is not.

```bash
# option (b): fresh, no path coupling
docker run --rm \
  -v "$(pwd)/data:/opt/s2fn/data" \
  -v "$(pwd)/warehouse:/opt/s2fn/warehouse" \
  -e SCOPE=mvp \
  s2-field-ndvi:latest \
  bash -c 'cd src && $SPARK_HOME/bin/spark-submit --master "local[*]" 01_fields.py && $SPARK_HOME/bin/spark-submit --master "local[*]" 02_scenes.py'
```

## 4. Benchmark matrix

Order and rows from `docs/k8s-runbook.md` rung (c). Fill wall-clock and cost only
from what the run actually measures — never backfill a "?" with a guess.

### Capacity model inputs (measured + estimated, labeled)

- **~69 s/scene, in-region observed bound** — this is the number this run exists
  to confirm; treat it as the working estimate driving the budget below, not a
  measured fact until row 1 lands.
- 41 season scenes (mvp scope) — measured, `run5.log`: "scenes: 41 selected".
- 12 event scenes (mvp_event scope: 6 tiles x 2 dates) — from `config.yml`'s
  `scopes.mvp_event`.
- `wall_clock ~= scenes x per_scene_cost / (cores / 4)` per `docs/spark-notes.md`
  "Capacity model" — cores here = 32 (m6i.8xlarge).

### Matrix rows

| Order | Scope | `per_scene` | `scl_tile_skip` | Notes / mitigation | Est. wall-clock | Est. cost @ $0.65/hr spot |
|---|---|---|---|---|---|---|
| 1 | mvp | off | off | baseline batched run. This is the row most likely to repeat run 5's TaskResultLost if the fields-pruning fix isn't actually exercised at off/off — watch for it. Bump `DRIVER_MEM=12g` and add `spark.driver.maxResultSize=4g` (up from Spark's 1g default) as the batch-mode broadcast mitigation if the driver looks memory-pressured. | ~59 min (41 x 69s / 8 effective cores at local[4]-equivalent parallelism scaled to 32 vCPU — reconcile against measured local[4] first) | ~$0.65 |
| 2 | mvp | on | off | memory-safety lever per `docs/spark-notes.md` — expect SLOWER than row 1 at demo scope (~10x); mvp scope is the first real test of whether that ratio holds or shrinks with real per-scene work. | unknown — do not extrapolate the demo-scope 10x onto mvp before this row lands | ? |
| 3 | mvp | off | on | cloudier real scope than the demo pair's near-clear skies — first real test of whether the SCL pre-pass finally pays off (it regressed 19% at demo scope). | unknown | ? |
| 4 | mvp | on | on | untested combination, both flags in `docs/spark-notes.md` explicitly. | unknown | ? |
| 5 | demo | off (python_udf engine) | off | `ndvi_engine: python_udf` retry: Recipe 1 from `docs/sedona-udf-memory-notes.md` — `--master local[4] --driver-memory 32g --conf spark.python.worker.memory=1g`; if it OOMs, Recipe 2 — `--master local[2] --driver-memory 48g` (+ optionally 128px tiles, ~4x smaller rows). Root cause: single-row pickle transport of a UDT (Arrow knobs are inert on PySpark 3.5); heap is the only real lever. If Recipe 2 fails, heap-dump and file upstream (no known apache/sedona issue exists for this). | unknown | ? |
| 5b | demo | off (python_udf engine, TILED) | off | the official mitigation arm (researched 2026-08-08): RS_MapAlgebra/jiffle is deprecated upstream as of 1.9.1 (sedona#3214), python_udf is the sanctioned path, and Sedona's own answer to big-row JVM-to-Python transport is smaller rasters per row — raster reader `retile`/`tileWidth`/`tileHeight` (added 1.9.0 explicitly for OOM avoidance) or `RS_TileExplode`, then per-tile sum/count rolled up. Run at the SAME modest heap as the jiffle baseline (not the 32g recipe) — the interesting result is whether tiling alone closes the memory gap. Three-way readout: jiffle vs python_udf-bare vs python_udf-tiled. | unknown | ? |
| 6 | mvp_event | off | off | derecho pair across the 6 mvp tiles — the DiD sample multiplier (12 event scenes vs 2 at demo scope, 6x the field x wind-band coverage for the DiD table). | 12 x 69s / 8 ≈ 17 min | ~$0.19 |

Row-1 wall-clock is a back-of-envelope extrapolation from the 69s/scene bound and
should be treated as a placeholder to replace with the actual row-1 measurement
before trusting rows 2-6's cost column, which are left as "?" deliberately —
`docs/build-plan.md`'s own rule, "never present modeled numbers as measured,"
applies here too.

### Total run budget

**Target: under $5. Hard ceiling: 6h wall-clock, self-terminate (same failsafe
pattern as run 5).** If row 1 alone blows past ~90 minutes, stop and reassess
before burning the ceiling on rows 2-6 — that's a sign the 69s/scene bound doesn't
hold at mvp scope and the whole matrix's estimates need redoing, not a reason to
push through on a stale budget.

Execute each row:

```bash
export SCOPE=mvp DRIVER_MEM=12g
docker run --rm \
  -v "$(pwd)/data:/opt/s2fn/data" \
  -v "$(pwd)/warehouse:/opt/s2fn/warehouse" \
  -e SCOPE -e DRIVER_MEM \
  s2-field-ndvi:latest \
  bash -c 'cd src && $SPARK_HOME/bin/spark-submit --master "local[*]" \
    --driver-memory "$DRIVER_MEM" \
    --conf spark.driver.maxResultSize=4g \
    03_ndvi_zonal.py && echo RUN_COMPLETE $(date -u +%T)'
```

**`--driver-memory` is not optional here, and passing `-e DRIVER_MEM` alone is a
trap** (this snippet had it wrong until run 6 paid for the lesson): `session.py`
sets `spark.driver.memory` on the builder, which only reaches the JVM when pyspark
launches it — the bare-`python` Makefile path. Under `spark-submit` the JVM is
already running, the builder value is ignored, and the driver silently gets Spark's
**1g default**. Row 1 OOMed identically at a claimed "12g" and "64g" because both
were really 1g. Confirm the real heap in the log before trusting a run:
`grep -m1 "MemoryStore started" run6-row1.log` — capacity is ~0.6 x the heap
(24g -> 14.2 GiB). `session.py` now warns when `DRIVER_MEM` is set under
spark-submit.

Toggle `raster.per_scene` / `raster.scl_tile_skip` / `raster.ndvi_engine` in
`config.yml` between rows (or via whatever env override the pipeline supports —
check `src/config.py` before assuming one exists; none was found in this read).

## 5. Run hygiene

All of the following are lessons paid for in runs 4/5, not defensive boilerplate.

### Success marker must gate on exit code

Run 5's wrapper printed `RUN_COMPLETE 03:40:28` in `run5.log` (confirmed,
line 645) **after** a fatal Python `Traceback` and a `Py4JJavaError` — because the
wrapper used a bare `;` between the job and the echo, which runs regardless of
exit status. This is the exact failure mode of a green-looking log that actually
died. Corrected pattern, `&&` not `;`:

```bash
$SPARK_HOME/bin/spark-submit --master "local[*]" 03_ndvi_zonal.py \
  && echo "RUN_COMPLETE $(date -u +%T)" \
  || echo "RUN_FAILED $(date -u +%T) exit=$?"
```

Never trust a `RUN_COMPLETE` grep alone without also grepping for its failure
counterpart — see next.

### Monitor greps the union of success AND failure signatures

```bash
tail -f run6.log | grep -E "RUN_COMPLETE|RUN_FAILED|Traceback|TaskResultLost|OutOfMemoryError|ERROR TaskSetManager|Job aborted"
```

A monitor that only greps `RUN_COMPLETE` cannot distinguish "still running" from
"died silently past the point where the wrapper would have logged" — grep the
failure signatures too, every one seen in run 4/5's logs (`Traceback`,
`TaskResultLost`, `OutOfMemoryError`, `Job aborted due to stage failure`).

### Results sync BEFORE teardown

```bash
tar czf run6-warehouse.tar.gz -C warehouse .
aws s3 cp run6-warehouse.tar.gz s3://<scratch-bucket>/s2fn-run6/ --region us-west-2
aws s3 cp run6.log s3://<scratch-bucket>/s2fn-run6/
scp -i <keypair>.pem ec2-user@<instance-ip>:~/s2fn/run6*.log .   # local copy too, belt and suspenders
```

Do this before any teardown step below. Once the instance terminates, anything
not synced is gone — this is what makes `mvp-warehouse.tar.gz` "salvage" in the
first place (section 3).

### Full teardown checklist (run-5 pattern)

```bash
aws ec2 terminate-instances --instance-ids <instance-id> --region us-west-2
aws ec2 wait instance-terminated --instance-ids <instance-id> --region us-west-2
aws ec2 delete-security-group --group-id <sg-id> --region us-west-2
aws ec2 delete-key-pair --key-name <keypair> --region us-west-2
rm -f <keypair>.pem
aws ec2 describe-instances --region us-west-2 \
  --filters "Name=tag:Name,Values=s2fn-run6" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output table
```

The final `describe-instances` is the verification step, not optional — confirm
`terminated` (or no results) before considering teardown done. Also check for
orphaned EBS volumes if the instance was force-terminated rather than cleanly
stopped (`--block-device-mappings` above did not set `DeleteOnTermination`
explicitly; default behavior for the root volume is delete-on-termination, but
verify with `aws ec2 describe-volumes --filters Name=status,Values=available`
after teardown).

### 6h self-terminate failsafe at launch

Set this in user-data at instance launch, not as an afterthought:

```bash
--user-data '#!/bin/bash
echo "shutdown -h now" | at now + 6 hours'
```

(`at` needs to be installed/enabled on the chosen AMI — verify, or use
`shutdown -h +360` directly in user-data as the simpler equivalent that needs no
`atd` service.) This is the same ceiling stated in section 4's run budget — it
exists so a hung or looping row cannot silently run the on-demand fallback price
past the point anyone notices.

## 6. What lands where afterward

- Measured matrix rows (section 4) → `docs/spark-notes.md`'s "Flag economics" and
  "Capacity model" sections, and `docs/k8s-runbook.md` rung (c)'s scaling table
  and `per_scene`/`scl_tile_skip` matrix — replace the `?` cells, not add a new
  section.
- The mvp row of `README.md`'s "Measured performance and cost" table (currently
  `~$2 spot / in progress`, line 177) → real wall-clock and cost, status flips to
  `measured`.
- **Ordering (Ross's standing instruction): all of the above land on `main`
  first, before `opt/phase1` merges.** Do not carry these doc updates as part of
  the `opt/phase1` branch's own diff — commit them to `main` directly (or a
  short-lived branch off `main`) once the run's numbers are in hand, independent
  of whatever else is in flight on `opt/phase1`.
