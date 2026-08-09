# Base pinned per docs/build-plan.md "Stack (pin exactly)": apache/spark-py and
# bitnami/spark are dead, this is the maintained multi-arch image (works on M4 + kind).
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

# Probed on this exact tag (2026-08-08, `docker run --rm <base> bash -c '...'`):
#   python3 --version -> 3.10.12   (src/session.py needs 3.11, installed below)
#   $SPARK_HOME       -> /opt/spark
#   id                -> uid=185(spark) gid=185(spark)  (base image's default
#                        USER is already "spark" -- USER 185 at the end of this
#                        file is re-asserting the pin, not introducing it)
#   OS                -> Ubuntu 22.04.5 LTS (jammy)

USER root

# python3.11: src/session.py's assert_versions() requires (3, 11) exactly and
# that assert is not being relaxed. jammy-updates/universe's python3.11 is
# 3.11.0~rc1 (a 2022 pre-release, verified via `apt-cache madison`), so pull
# the real point release from deadsnakes instead -- probed available for both
# arm64 and amd64 on jammy: 3.11.15-1+jammy1. python3.11-distutils and
# python3.11-venv are dropped: `apt-cache madison python3.11-distutils`
# returns nothing (apt silently satisfies the name from the base's existing
# python3-distutils 3.10 package instead -- a no-op, not a real install) and
# python3.11-venv is unused (no venv is created in this image). python3-pip
# is already installed for the base's 3.10 and lives under the shared
# /usr/lib/python3/dist-packages path, so `python3.11 -m pip` works
# immediately -- verified with a real `pip install` and `pip download` for
# every requirements.txt package (all resolve to cp311/abi3/py3-none
# manylinux wheels for aarch64, no compiler needed, so no -dev/build-essential
# packages are pulled in here). software-properties-common/gnupg are only
# needed to register the PPA and are purged again in the same layer.
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends \
        software-properties-common gnupg \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends python3.11 \
    && apt-get purge -y -qq software-properties-common gnupg \
    && apt-get autoremove -y -qq \
    && rm -rf /var/lib/apt/lists/*

# Jars baked into $SPARK_HOME/jars, never `--packages`/`spark.jars.packages` at
# job start (kind/EKS executor pods have no guaranteed route to Maven Central
# mid-job, and ivy resolution there is slow and flaky). Every URL below was
# checked with `curl -sI` (200, content-type application/java-archive) against
# Maven Central on 2026-08-08 before being pinned here.
#
# sedona-spark-shaded MUST be 1.9.1, not 1.9.0 (1.9.0 has the >180m ST_Transform
# regression, GH-3161 -- silent corruption of every Iowa reprojection):
#   https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.9.1/sedona-spark-shaded-3.5_2.12-1.9.1.jar
# geotools-wrapper is REQUIRED for raster (RasterUDT is GeoTools GridCoverage2D;
# missing jar = NoClassDefFoundError at first raster call):
#   https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.9.1-33.5/geotools-wrapper-1.9.1-33.5.jar
# Iceberg table format runtime:
#   https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar
# hadoop-aws MUST match the Hadoop line pyspark 3.5.3 is built against -- 3.3.4,
# confirmed by the base image already shipping hadoop-client-api/runtime 3.3.4
# in $SPARK_HOME/jars:
#   https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
# aws-java-sdk-bundle pairs with hadoop-aws 3.3.4 (S3AFileSystem's expected SDK
# version; unshaded/mismatched SDK jars are the classic hadoop-aws footgun):
#   https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
RUN cd "$SPARK_HOME/jars" \
    && curl -fsSL -O https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.9.1/sedona-spark-shaded-3.5_2.12-1.9.1.jar \
    && curl -fsSL -O https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.9.1-33.5/geotools-wrapper-1.9.1-33.5.jar \
    && curl -fsSL -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
    && curl -fsSL -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Cluster-level Spark defaults (s3a anon credentials, fadvise, event log off).
# spark-submit auto-loads $SPARK_HOME/conf/spark-defaults.conf with no extra
# flag when spark-submit runs INSIDE this image directly (e.g. a manual
# `docker run ... spark-submit --master local[1] ...` smoke check). It does
# NOT apply on k8s (kind/EKS, native submit or spark-operator): Spark on
# Kubernetes generates a ConfigMap from the submit-time config and mounts it
# over SPARK_CONF_DIR in the driver/executor pods, shadowing whatever this
# file bakes (verified: /opt/spark/conf doesn't exist at all in the base
# image, and spark-kubernetes' Client/BasicExecutorFeatureStep classes are
# what write SPARK_CONF_DIR_INTERNAL). src/session.py sets the same s3a keys
# at the SparkConf-builder level, which is what actually takes effect on k8s.
COPY docker/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"

# Do NOT pip-install pyspark in this image. This image already IS Spark
# 3.5.9 ($SPARK_HOME/python/lib/pyspark.zip): spark-submit's PythonRunner
# prepends that zip to PYTHONPATH ahead of site-packages at launch, so a
# pip-installed pyspark package is never actually imported here -- it would
# just be a dead ~450MB (pyspark 3.5.3 on PyPI is an sdist-only 317MB source
# build, no wheel) that also created a version mismatch against
# assert_versions() in baked mode (see src/session.py). requirements.txt
# pins pyspark==3.5.3 for the LOCAL venv (`make setup`), so filter that one
# line out before installing the rest here; apache-sedona is listed with no
# [spark] extra (see requirements.txt's own comment) so it does not pull
# pyspark back in as a transitive dependency.
COPY requirements.txt /opt/s2fn/requirements.txt
RUN grep -v '^pyspark' /opt/s2fn/requirements.txt > /opt/s2fn/requirements-image.txt \
    && python3.11 -m pip install --no-cache-dir -r /opt/s2fn/requirements-image.txt

# Code last so a source edit doesn't invalidate the jar/dependency layers above.
COPY src/ /opt/s2fn/src/
COPY scripts/ /opt/s2fn/scripts/
COPY config.yml /opt/s2fn/config.yml

ENV PYSPARK_PYTHON=/usr/bin/python3.11 \
    PYTHONPATH=/opt/s2fn/src \
    S2FN_JARS_BAKED=1

# uid 185 needs to write the Iceberg warehouse (config.yml: iceberg.warehouse
# resolves to $REPO_ROOT/warehouse = /opt/s2fn/warehouse), web/ (04_publish.py
# writes web/season.json + web/county.geojson; Path.write_text does not mkdir
# its parent, so stage 04 would otherwise raise FileNotFoundError in-container)
# and Spark's local scratch dir; /tmp is already world-writable (drwxrwxrwt)
# in the base image.
RUN mkdir -p /opt/s2fn/warehouse /opt/s2fn/web && chown -R 185:185 /opt/s2fn

USER 185
WORKDIR /opt/s2fn
