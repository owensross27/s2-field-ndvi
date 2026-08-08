"""SparkSession builder. One place for jar pins, Iceberg catalog, and s3a config.

Pins (verified 2026-08-07, see docs/build-plan.md -- do not bump casually):
  sedona 1.9.1 (1.9.0 has the >180m ST_Transform regression, GH-3161)
  geotools-wrapper 1.9.1-33.5 (required for raster; RasterUDT is GridCoverage2D)
  iceberg-spark-runtime-3.5_2.12 1.11.0
"""
import os
import sys
from pathlib import Path

from pyspark import SparkConf
from sedona.spark import SedonaContext

from config import ICEBERG, REPO_ROOT

SEDONA = "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.1"
GEOTOOLS = "org.datasyslab:geotools-wrapper:1.9.1-33.5"
ICEBERG_JAR = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0"
# s3a filesystem for the raster reader; MUST match pyspark 3.5.x's Hadoop (3.3.4)
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"

# Filenames of the same five pins above (SEDONA/GEOTOOLS/ICEBERG_JAR/HADOOP_AWS
# plus aws-java-sdk-bundle, which docker/Dockerfile also bakes but which has no
# top-level constant here since nothing else references its coordinate), as
# baked into $SPARK_HOME/jars by docker/Dockerfile. Used only by
# assert_versions() in baked mode (see below).
_BAKED_JAR_FILES = (
    "sedona-spark-shaded-3.5_2.12-1.9.1.jar",
    "geotools-wrapper-1.9.1-33.5.jar",
    "iceberg-spark-runtime-3.5_2.12-1.11.0.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
)

# Set by docker/Dockerfile (kind/EKS image). Baked-jar containers must skip
# spark.jars.packages: those jars are already on the classpath via
# $SPARK_HOME/jars, and resolving them again via ivy at job start is both
# redundant and unsafe (executor pods have no guaranteed route to Maven
# Central mid-job). Local/dev runs never set this env var, so behavior there
# is unchanged.
JARS_BAKED = os.environ.get("S2FN_JARS_BAKED") == "1"


def get_sedona(app_name: str = "s2-field-ndvi", master: str | None = None):
    builder = SedonaContext.builder().appName(app_name)
    if not JARS_BAKED:
        builder = builder.config(
            "spark.jars.packages", ",".join([SEDONA, GEOTOOLS, ICEBERG_JAR, HADOOP_AWS])
        )
    builder = (
        builder
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG['catalog']}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG['catalog']}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG['catalog']}.warehouse",
                os.environ.get("S2FN_WAREHOUSE") or str(REPO_ROOT / ICEBERG["warehouse"]))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.driver.memory", os.environ.get("DRIVER_MEM", "6g"))
        # anonymous access to the public Sentinel-2 buckets; random fadvise for COGs
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")
        # macOS: unbounded s3a connections + 10 local tasks exhaust kernel socket
        # buffers ("No buffer space available") and the python workers die with EOF
        .config("spark.hadoop.fs.s3a.connection.maximum", "24")
    )
    # master resolution: explicit arg > SPARK_MASTER env > whatever spark-submit set
    # > local[*]. Never override a master that spark-submit (k8s/EKS) already provided.
    master = master or os.environ.get("SPARK_MASTER")
    if master:
        builder = builder.master(master)
    elif not SparkConf().contains("spark.master"):
        builder = builder.master("local[4]")  # not [*]: 16GB laptop = 6g JVM + 4 python
        # workers; more cores memory-pressure-kills workers (EOFError, no stacktrace)
    return SedonaContext.create(builder.getOrCreate())


def _assert_baked_jars() -> None:
    """Baked mode never sets spark.jars.packages (see get_sedona), so there is
    no packages string to check. Verify the exact pinned jar files landed in
    $SPARK_HOME/jars instead -- same guarantee (these exact versions, nothing
    else resolved), different source of truth."""
    jars_dir = Path(os.environ.get("SPARK_HOME", "/opt/spark")) / "jars"
    missing = [f for f in _BAKED_JAR_FILES if not (jars_dir / f).exists()]
    assert not missing, f"baked jars missing from {jars_dir}: {missing}"


def assert_versions(sedona) -> None:
    """Fail loudly if the resolved stack is not the pinned one."""
    import pyspark
    if JARS_BAKED:
        # Baked containers never pip-install pyspark (docker/Dockerfile skips
        # it -- Spark's PythonRunner prepends $SPARK_HOME/python/lib/pyspark.zip
        # to PYTHONPATH at spark-submit launch, so the image's own pyspark
        # 3.5.9 is what actually imports here, not a pinned point release).
        # Check the 3.5.x line instead of the exact local-mode pin.
        assert pyspark.__version__.startswith("3.5."), \
            f"pyspark {pyspark.__version__} not on the 3.5.x line"
    else:
        assert pyspark.__version__ == "3.5.3", f"pyspark {pyspark.__version__} != 3.5.3"
    ver = sys.version_info
    assert (ver.major, ver.minor) == (3, 11), f"python {ver.major}.{ver.minor} != 3.11"
    if JARS_BAKED:
        _assert_baked_jars()
        return
    jars = sedona.sparkContext.getConf().get("spark.jars.packages", "")
    assert "sedona-spark-shaded-3.5_2.12:1.9.1" in jars, jars
    assert "geotools-wrapper:1.9.1-33.5" in jars, jars


def _demo() -> None:
    """ponytail check: exercises the baked-jar branch without a JVM/Spark
    session (get_sedona() needs a real cluster; this is the part that doesn't)."""
    import tempfile

    global JARS_BAKED
    saved = JARS_BAKED
    saved_home = os.environ.get("SPARK_HOME")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            jars_dir = Path(tmp) / "jars"
            jars_dir.mkdir()
            os.environ["SPARK_HOME"] = tmp
            JARS_BAKED = True
            try:
                _assert_baked_jars()
            except AssertionError:
                pass
            else:
                raise SystemExit("expected AssertionError: jars dir is empty")
            for fname in _BAKED_JAR_FILES:
                (jars_dir / fname).touch()
            _assert_baked_jars()  # must not raise now that all five exist
    finally:
        JARS_BAKED = saved
        if saved_home is None:
            os.environ.pop("SPARK_HOME", None)
        else:
            os.environ["SPARK_HOME"] = saved_home
    print("session.py: baked-jar assert_versions check ok")


if __name__ == "__main__":
    _demo()
