# Kubernetes runbook: kind -> EKS

Companion to `docs/build-plan.md` ("Compute + cost", "Spark-experience signal") and
`docs/spark-notes.md`. Three rungs: (a) kind on this laptop, primary path first, operator
appendix second; (b) the EKS translation (documented, not run — no AWS calls from this
worktree); (c) what to measure once a real run happens.

**Status (2026-08-08)**: rung (a) is **verified end to end** via the native
spark-submit path on a real kind cluster: driver + separate executor pod, k8s scheduler
backend confirmed in the driver log, `SCOPE=demo` confirmed in the pod env, and the
full 03 stage recomputed `field_ndvi` (+13,369 rows, 2 scenes in 1025s — see rung (c)).
Two traps were hit and fixed on the way; both are documented at their point of use in
step 5 below: the client-side JAVA_HOME fallback, and — the big one — a `src/session.py`
bug where the local[4] fallback silently clobbered the k8s master *inside the driver
pod*, running the whole job single-pod and OOMKilling it (fixed; the guard now keys on
`PYSPARK_GATEWAY_PORT`). `make image` is step 0 of any attempt; every command below,
plus `k8s/sparkapplication.yml`'s `image:` field and `Makefile`'s `image` target,
assumes the `:latest` tag it produces. Rung (b) is translation-only, per the task
boundary: no `eksctl`/AWS command in this doc has been run.

## Rung (a): kind

### 0. Prerequisites (versions this was verified against)

Docker 29.1.3 (running, Docker Desktop VM at 10GB per `docs/build-plan.md`), kind v0.32.0,
kubectl v1.36.3 client, helm v4.2.3. `docker/Dockerfile` builds on this M4 in ~100s
(mostly `pip install` of geopandas/rasterio/great-expectations wheels — all prebuilt
manylinux aarch64 wheels, no compiler step).

**Memory budget, worth knowing before you hit it**: Spark's k8s memory-overhead factor
defaults to 0.4 (not 0.1) for non-JVM pods, which this Python job's driver/executors are.
`k8s/sparkapplication.yml` requests 1 driver (2g) + 1 executor (2g), so real pod requests
are ~2.8Gi + 2.8Gi = ~5.6Gi, plus kind's own 2-node control-plane overhead, in a 10GB
Docker Desktop VM already running the operator and its webhook — still tight. The native
`spark-submit` path in step 5 below sets `spark.executor.instances=2`, which would push
the same math to ~8.4Gi; drop it to 1 (or raise the VM's memory in Docker Desktop
settings) if pods go `Evicted` or `OOMKilled` — the point of this run is exercising the
submission path, not throughput.

### 1. Build the image

```bash
make image        # docker build -t s2-field-ndvi:latest -f docker/Dockerfile .
```

Verified this session: the built image runs as uid 185, `WORKDIR /opt/s2fn`,
`PYSPARK_PYTHON=/usr/bin/python3.11`, `S2FN_JARS_BAKED=1`, and
`config.REPO_ROOT == Path("/opt/s2fn")` (checked by running `python3.11 -c "import
config; print(config.REPO_ROOT)"` inside the built image with the same volume mounts
rung 3 below sets up).

### 2. Prep the two host mount directories

```bash
cd /Users/ross/s2fn-opt   # everything below assumes this cwd
mkdir -p data warehouse-k8s
chmod 777 warehouse-k8s
```

`data/` should already exist from `make data`; leave its permissions alone (755 is fine,
read-only is all any container needs from it). `warehouse-k8s` is a **new, dedicated**
directory — not the laptop's `./warehouse` — for a reason worth understanding before
touching either file:

A Hadoop-catalog Iceberg table's `metadata.json` embeds an **absolute** `location` path
recorded at table-creation time. Checked against a real table in this worktree:

```
$ python3 -c "import json; print(json.load(open('warehouse/crop/scenes/metadata/...metadata.json'))['location'])"
/Users/ross/s2fn-opt/warehouse/crop/scenes
```

Mount that directory into a container whose `REPO_ROOT` is `/opt/s2fn` and Iceberg will
still look for data files under `/Users/ross/s2fn-opt/...` inside the pod — nothing there
— `FileNotFoundException`. `docs/spark-notes.md` already flags this exact trap ("Never
copy a Hadoop-catalog Iceberg warehouse"). `warehouse-k8s/` sidesteps it by starting
empty and only ever being written to by processes whose `REPO_ROOT` is `/opt/s2fn` (the
container path, both for the pre-population step below and every pod after it) — so its
absolute-path lineage never crosses the laptop venv's.

The `chmod 777` is defensive, kept even though it's a no-op on this Mac: Docker Desktop's
macOS bind mounts are writable by any container uid regardless of the host-side
permission bits, so uid 185 can write into `warehouse-k8s/` here whether or not the
chmod runs. It matters on a real Linux host or an EKS-style node, where hostPath
ownership on the node filesystem is actually enforced and a directory kind/kubelet
auto-creates as `root:root` would otherwise block uid 185's Iceberg write with a plain
permission error. Keep the chmod so this doc's sequence doesn't silently depend on
Docker Desktop's mount semantics.

### 3. Create the cluster and load the image

`k8s/kind-cluster.yml`'s `./data`/`./warehouse-k8s` mounts resolve against the invoking
shell's cwd with no guard beyond this comment — from the wrong cwd, Docker silently
auto-creates empty dirs at the wrong location instead of failing loudly, and the job
dies much later on a missing Iceberg table instead of at cluster-create time:

```bash
[ -f k8s/kind-cluster.yml ] || { echo "run from the repo root (see step 2)" >&2; exit 1; }
kind create cluster --config k8s/kind-cluster.yml   # ~15-30s to Ready, from repo root
kind load docker-image s2-field-ndvi:latest --name s2fn   # ~70s for this image's size
```

Both nodes get `./data` and `./warehouse-k8s` mounted (see `k8s/kind-cluster.yml`'s
header comment for the full path contract) — mounted on both control-plane and worker
since either could get a pod scheduled, and the mount is nearly free either way.

### 4. RBAC for native spark-submit (verified against Spark 3.5.9's own docs)

```bash
kubectl create serviceaccount spark -n default
kubectl create clusterrolebinding spark-role \
  --clusterrole=edit --serviceaccount=default:spark
```

(`ClusterRoleBinding` is cluster-scoped by definition, so a `--namespace` flag on the
`create clusterrolebinding` command above would be inert — omitted.)

This binds `edit` cluster-wide, broader than the driver actually needs (it only ever
creates executor pods in its own namespace) — a deliberate laptop-convenience choice,
not a narrow grant: it's Spark 3.5.9's own documented example and was used as-is in a
prior session's run, where the driver pod launched, was scheduled, and pulled the
kind-loaded image. (Not verified: the same service account creating *executor* pods, or
anything past that point — see the Status note at the top of this doc.) For EKS, where
cluster-wide `edit` is a real over-grant, prefer a namespaced `Role` (pods/services/
configmaps/persistentvolumeclaims: create,get,list,watch,delete) + `RoleBinding` instead.

### 5. Native spark-submit (the primary path)

```bash
API=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')

# IMPORTANT: do NOT `source scripts/java_env.sh` before this command (or any
# spark-submit --master k8s:// invocation). That script sets PYSPARK_PYTHON to the
# laptop venv's absolute path for LOCAL runs; spark-submit reads that from the
# CLIENT's environment and bakes it into the driver pod's launch config, so the
# in-pod driver tries to exec /Users/ross/s2fn-opt/.venv/bin/python -- a path that
# only exists on the Mac. Confirmed by hitting this exact failure in this session:
#   Exception in thread "main" java.io.IOException: Cannot run program
#   "/Users/ross/s2fn-opt/.venv/bin/python": error=2, No such file or directory
# You do still need JAVA_HOME on the CLIENT (spark-submit itself is a JVM launcher).
# /usr/libexec/java_home fails on this Mac (no system JDK registered); Homebrew's
# keg-only openjdk@17 is what scripts/java_env.sh itself falls back to -- use that
# fallback directly, WITHOUT sourcing the script (see the PYSPARK_PYTHON trap above):
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
unset PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON
export PATH="$(pwd)/.venv/bin:$PATH"   # so `spark-submit` resolves to pyspark 3.5.3's
                                        # bundled client scripts, not a system python/spark

spark-submit \
  --master "k8s://$API" \
  --deploy-mode cluster \
  --name s2fn-demo \
  --conf spark.kubernetes.container.image=s2-field-ndvi:latest \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.namespace=default \
  --conf spark.executor.instances=2 \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  --conf spark.kubernetes.driverEnv.SCOPE=demo \
  --conf spark.executorEnv.SCOPE=demo \
  --conf spark.kubernetes.driver.volumes.hostPath.warehouse.mount.path=/opt/s2fn/warehouse \
  --conf spark.kubernetes.driver.volumes.hostPath.warehouse.options.path=/mnt/s2fn-warehouse \
  --conf spark.kubernetes.executor.volumes.hostPath.warehouse.mount.path=/opt/s2fn/warehouse \
  --conf spark.kubernetes.executor.volumes.hostPath.warehouse.options.path=/mnt/s2fn-warehouse \
  --conf spark.kubernetes.driver.volumes.hostPath.data.mount.path=/opt/s2fn/data \
  --conf spark.kubernetes.driver.volumes.hostPath.data.options.path=/mnt/s2fn-data \
  --conf spark.kubernetes.executor.volumes.hostPath.data.mount.path=/opt/s2fn/data \
  --conf spark.kubernetes.executor.volumes.hostPath.data.options.path=/mnt/s2fn-data \
  local:///opt/s2fn/src/03_ndvi_zonal.py
```

The `--conf spark.kubernetes.{driver,executor}.volumes.hostPath.<name>.*` flags are the
native spark-submit equivalent of `k8s/sparkapplication.yml`'s `volumes`/`volumeMounts`
blocks — same two mounts, same path contract, expressed as `--conf` instead of YAML
because this path has no CRD to hold them.

Watch it: `kubectl logs -n default -f <pod-name>-driver` (get the name from the
`spark-submit` status output, or `kubectl get pods -n default`). Don't assume the
`SCOPE` conf keys above took effect — `kubectl exec <pod-name>-driver -- env |
grep ^SCOPE=` to confirm the env var actually reached the pod rather than
silently falling back to `src/config.py`'s `demo` default.

### 6. Populate the warehouse before trusting a real run

`03_ndvi_zonal.py` reads `local.crop.fields` and `local.crop.scenes` — tables that
`01_fields.py`/`02_scenes.py` write, not `03`. Run those two stages **through the same
image**, so their Iceberg writes land under the container-native `/opt/s2fn` path
(matching `warehouse-k8s/`'s empty, container-only lineage from step 2) rather than
under the laptop venv's `REPO_ROOT`:

```bash
docker run --rm \
  -v "$(pwd)/data:/opt/s2fn/data" \
  -v "$(pwd)/warehouse-k8s:/opt/s2fn/warehouse" \
  -e SCOPE=demo \
  s2-field-ndvi:latest \
  bash -c 'cd src && $SPARK_HOME/bin/spark-submit --master "local[4]" 01_fields.py && $SPARK_HOME/bin/spark-submit --master "local[4]" 02_scenes.py'
```

Plain `python3.11 01_fields.py` does NOT work here: `docker/Dockerfile` deliberately
never pip-installs pyspark into this image (see its "Do NOT pip-install pyspark"
comment), and its static `PYTHONPATH=/opt/s2fn/src` doesn't include `$SPARK_HOME/python`
(the base image ships no `PYTHONPATH` of its own to inherit from). Only `spark-submit`'s
PythonRunner prepends `$SPARK_HOME/python` (the image's bundled pyspark) at launch —
`python3.11 -c "import pyspark"` inside this image raises `ModuleNotFoundError: No
module named 'pyspark'`.

`02_scenes.py` needs live network access to the Earth Search STAC API (anonymous,
public) — normal, expected egress for this pipeline, not an AWS control-plane call.

### 7. Operator appendix

**Verified 2026-08-08**: the exact sequence below ran end to end on a real kind cluster
— helm install (ghcr pulls, ~2 min), CRD Established, webhook rolled out, manifest
applied, SparkApplication went RUNNING -> COMPLETED. Tip: with `field_ndvi` already
populated from the native-path run, the operator job hits 03's idempotent skip
("0 to process") and completes in seconds — a deliberately cheap way to validate the
CRD/operator/RBAC/volume plumbing without re-paying the ~17 min of raster compute.

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace --version 2.5.2

kubectl wait --for=condition=Established \
  crd/sparkapplications.sparkoperator.k8s.io --timeout=60s
kubectl rollout status deployment/spark-operator-webhook \
  -n spark-operator --timeout=120s
# if that deployment name doesn't match your release name, list it:
#   kubectl get deploy -n spark-operator

kubectl apply -f k8s/sparkapplication.yml
kubectl get sparkapplication s2-field-ndvi-demo -n default -w
```

`k8s/sparkapplication.yml`'s `serviceAccount: spark-operator-spark` assumes the release
name `spark-operator` above (the chart auto-creates a `Role`/`RoleBinding`/
`ServiceAccount` named `<release>-spark` in every `spark.jobNamespaces` namespace,
default `["default"]` — confirmed from the chart's own
`templates/spark/{rbac,serviceaccount}.yaml` at tag `v2.5.2`, not guessed). If you use a
different release name, `kubectl get sa -n default | grep spark` and fix the manifest.

Every field name in `k8s/sparkapplication.yml` was checked against the real v2.5.2 CRD
(`sparkoperator.k8s.io/v1beta2`, downloaded from
`raw.githubusercontent.com/kubeflow/spark-operator/v2.5.2/.../sparkoperator.k8s.io_sparkapplications.yaml`)
and passed `kubectl apply --dry-run=server` against that CRD installed on a real kind
cluster in this session.

### 8. Teardown

```bash
kind delete cluster --name s2fn
```

Nothing else to clean up locally — `warehouse-k8s/` and `data/` are gitignored
(`.gitignore` was extended with `warehouse-k8s/`; `data/` was already covered) and can be
`rm -rf`'d and rebuilt any time via steps 2 and 6.

## Rung (b): EKS translation (documented only — nothing here has been run)

```bash
eksctl create cluster \
  --name s2fn-state --region us-west-2 \
  --nodegroup-name spot-ndvi --nodes 3 \
  --spot --instance-types=m6i.4xlarge
```

(`--spot --instance-types=...` is eksctl's own documented spot-nodegroup form, not a
guess — confirmed against `docs.aws.amazon.com/eks/latest/eksctl/spot-instances.html`.)

**NAT gateway trap** (from `docs/build-plan.md`, worth repeating at the point of use):
a NAT gateway bills $0.045/GB, and the state-tier scope moves ~230GB — about $10, more
than the compute itself. Either put the node subnets on a **public subnet** (simplest for
a portfolio run that gets torn down same-day) or add a **free S3 Gateway VPC Endpoint** so
S3 traffic (the Sentinel-2/Landsat/DEM buckets, all on S3) never transits the NAT at all.
`eksctl create cluster` defaults to private+NAT; override explicitly.

**ECR push**:

The nodes above are `m6i.4xlarge` (amd64), but `make image` (and every image built on
this M4 laptop so far) is host-arch only — arm64. Pushing that image would kubelet-fail
the pull ("no matching manifest for linux/amd64") or exec-format-error the entrypoint.
Build for amd64 explicitly before tagging/pushing — `Makefile`'s `image-amd64` target
already does this via `buildx` + QEMU emulation, slower than the ~100s local build:

```bash
make image-amd64   # docker buildx build --platform linux/amd64 -t s2-field-ndvi:latest -f docker/Dockerfile .

aws ecr create-repository --repository-name s2-field-ndvi --region us-west-2
aws ecr get-login-password --region us-west-2 | \
  docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-west-2.amazonaws.com
docker tag s2-field-ndvi:latest <account-id>.dkr.ecr.us-west-2.amazonaws.com/s2-field-ndvi:latest
docker push <account-id>.dkr.ecr.us-west-2.amazonaws.com/s2-field-ndvi:latest
```

Then in `k8s/sparkapplication.yml`: `image:
<account-id>.dkr.ecr.us-west-2.amazonaws.com/s2-field-ndvi:latest`,
`imagePullPolicy: IfNotPresent` (drop `Never` — there's a real registry now), and the
nodes need an IAM role with `AmazonEC2ContainerRegistryReadOnly` (eksctl's default node
role includes this) instead of relying on kind's local image cache.

**Storage**: the two hostPath volumes go away entirely. `warehouse` moves to S3 —
`spark.sql.catalog.local.warehouse` becomes an `s3a://` URI, using the same anonymous-read
`spark.hadoop.fs.s3a.*` config already in `docker/spark-defaults.conf` for the *input*
buckets, but the *output* bucket (this pipeline's own warehouse) needs real write
credentials — an IAM role for the pod's service account (IRSA / pod identity), not
anonymous. `data/` (the small reference inputs: `iowa_fields.parquet`,
`wind_polygons`, `counties_500k`) either moves to the same S3 bucket under a `data/`
prefix, or gets baked into the image at build time (it's ~280MB, small enough) — baking
it in is the smaller diff and matches "jars baked, not pulled at job start" in spirit for
an ephemeral 6-12h-setup, same-day-teardown cluster. `sparkConf` no longer needs
`spark.kubernetes.namespace` if you're using a dedicated EKS namespace already scoped by
context; `driver`/`executor` `volumes`/`volumeMounts` in the SparkApplication get deleted
outright rather than translated.

**Same-day teardown (required, not optional)** — the control plane bills at $0.10/hr
while idle regardless of whether any job is running:

```bash
eksctl delete cluster --name s2fn-state --region us-west-2   # also takes the nodegroup + VPC/NAT
aws ecr delete-repository --repository-name s2-field-ndvi --region us-west-2 --force
```

`eksctl delete cluster` does not touch ECR — the repo created in the ECR push step above
keeps billing image storage (~4GB) indefinitely unless deleted separately.

## Rung (c): what to measure once a real run happens

All of this is currently unmeasured beyond the demo-scope numbers already in
`docs/spark-notes.md` (local[4], home broadband). Every row below needs a real kind or
EKS run first (see the Status note at the top of this doc).

### Scaling table (`docs/spark-notes.md`'s "Capacity model" section wants these rows)

Same mvp-scope job (`SCOPE=mvp`, ~90 scene-dekads per `docs/build-plan.md`), four
environments, wall-clock + $ per row:

| Environment | Cores | Wall-clock | Cost | Notes |
|---|---|---|---|---|
| kind, 1 driver + 1 executor (measured, demo scope not mvp) | 1 | 1025s / 2 scenes | $0 | 2026-08-08: 512s/scene at 1 executor core; per-core BETTER than local[4]'s 756 core-s/scene (Linux VM vs macOS). Distributed path verified: k8s scheduler backend + separate executor pod |
| local[4] (measured) | 4 | -- | $0 | already in spark-notes.md: 207s/scene, home broadband |
| local[10] | 10 | ? | $0 | same laptop, more cores -- watch for the EOFError ceiling |
| 16-vCPU EC2 on-demand (measured) | 16 | 150s / 2 scenes = 75s/scene | ~$0.03 for the pair | run 6, m6i.4xlarge us-west-2 in-region, demo scope. NOTE: mvp scope did NOT complete -- see spark-notes "mvp scaling wall"; the bottleneck is fields-per-scene, not cores |
| 3-node EKS (this repo's manifest) | ~48 (3x m6i.4xlarge-ish) | ? | ? | control-plane + first-run setup overhead, not just compute |

Fill wall-clock and $ only from real runs — `docs/build-plan.md`'s own rule: "never
present modeled numbers as measured."

### `per_scene` and `scl_tile_skip` benchmark matrix

`docs/spark-notes.md`'s "Flag economics" section measured both flags independently at
demo scope only (1 tile, 2 scenes) and explicitly left the combination and larger scopes
untested. The matrix to fill in on a cloud run:

| Scope | `per_scene` | `scl_tile_skip` | Wall-clock | Notes |
|---|---|---|---|---|
| demo | off | off | 375s (measured, laptop) | baseline |
| demo | off | on | 445s (measured, laptop) | 19% regression at demo scope, clear-sky pair |
| mvp | off | off | ? | |
| mvp | on | off | ? | memory-safety lever, not speed -- expect slower |
| mvp | off | on | ? | cloudier scope than demo -- does the pre-pass finally pay off? |
| mvp | on | on | ? | untested combination |

### jiffle vs python_udf head-to-head

`docs/spark-notes.md`: the python_udf engine (`ndvi_udf.py`) dies under load on macOS
local mode (kernel ENOBUFS on the JVM-to-worker socket) and has never been benchmarked —
only jiffle has real numbers. Linux (kind or EKS) removes that ceiling. Set
`raster.ndvi_engine: python_udf` in `config.yml` (or override via whatever env-driven
mechanism the pipeline supports) and run the same demo-scope job both ways:

| Engine | Wall-clock | Notes |
|---|---|---|
| jiffle (measured, laptop) | 207s/scene | current default; JVM-only, no python worker in the hot loop |
| jiffle (Linux/kind, measured) | 512s/scene at 1 executor core | 2026-08-08 demo scope; 512 core-s/scene vs local[4]'s ~756 -- per-core faster on Linux, wall-clock slower with 1 core |
| jiffle (in-region EC2, measured) | **75s/scene** at 16 vCPU | run 6, m6i.4xlarge us-west-2, 24g heap, 13,369 rows |
| python_udf (in-region EC2, measured) | **74s/scene** at 16 vCPU | **parity with jiffle**, identical 13,369 rows. The 1.9.1-sanctioned path (jiffle deprecated, sedona#3214) is free to adopt; its earlier macOS-ENOBUFS and 6g-heap OOM failures were memory-config artifacts, not engine limits |
| python_udf + 128px tiling (in-region EC2, measured) | 114s/scene at 16 vCPU | +54% for identical output -- retile/RS_TileExplode is a MEMORY lever, not a speed one; skip it when heap is adequate |

## Open questions for the orchestrator

1. **RESOLVED (2026-08-08)** — data loss during a prior session's validation
   (`rm -rf warehouse-k8s data` took the real `data/` with it). `wind_polygons` and
   `counties_500k` were restored via `scripts/fetch_data.sh`; `data/iowa_fields.parquet`
   is back as of 08-07 (253MB, present and used by the verified run). `data/publish/`
   remains regenerable via `04_publish.py` whenever needed.
2. **`sparkVersion: "3.5.9"` in the manifest matches the base image's bundled Spark JVM.**
   There is no separate pip-installed pyspark in this image at all — `docker/Dockerfile`
   deliberately skips it (see its "Do NOT pip-install pyspark" comment) — so there is no
   3.5.9/3.5.3 patch-version split on the Python side to reason about; the field is
   informational metadata for the operator. Worth a real check on a live run that nothing
   in the Sedona/Iceberg extension stack cares about the exact patch version beyond py4j
   wire compatibility.
3. **`docker/Dockerfile` doesn't copy `data/`** into the image (by design — it's meant to
   come from a mount/S3, matching this runbook), but also doesn't copy `docker/` itself or
   `web/`; nothing in this task needed those, flagging only in case another track assumed
   otherwise.
4. **RESOLVED (2026-08-08)** — both former gaps observed on a real cluster: the `spark`
   service account created an executor pod under the `edit` ClusterRoleBinding (separate
   `...-exec-1` pod Running alongside the driver, shuffle blocks served from it), and
   the operator appendix ran end to end (see step 7's Verified note). Item 2's "worth a
   real check" also happened implicitly: the 3.5.9-JVM/3.5.3-client pairing ran the full
   Sedona+Iceberg stack without any patch-version complaint.
