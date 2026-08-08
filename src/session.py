"""SparkSession builder. One place for jar pins, Iceberg catalog, and s3a config.

Pins (verified 2026-08-07, see docs/build-plan.md — do not bump casually):
  sedona 1.9.1 (1.9.0 has the >180m ST_Transform regression, GH-3161)
  geotools-wrapper 1.9.1-33.5 (required for raster; RasterUDT is GridCoverage2D)
  iceberg-spark-runtime-3.5_2.12 1.11.0
"""
import os
import sys

from pyspark import SparkConf
from sedona.spark import SedonaContext

from config import ICEBERG, REPO_ROOT

SEDONA = "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.1"
GEOTOOLS = "org.datasyslab:geotools-wrapper:1.9.1-33.5"
ICEBERG_JAR = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0"
# s3a filesystem for the raster reader; MUST match pyspark 3.5.x's Hadoop (3.3.4)
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"


def get_sedona(app_name: str = "s2-field-ndvi", master: str | None = None):
    builder = (
        SedonaContext.builder()
        .appName(app_name)
        .config("spark.jars.packages", ",".join([SEDONA, GEOTOOLS, ICEBERG_JAR, HADOOP_AWS]))
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG['catalog']}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG['catalog']}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG['catalog']}.warehouse",
                str(REPO_ROOT / ICEBERG["warehouse"]))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.driver.memory", "6g")  # heavy arrays live in the python workers
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


def assert_versions(sedona) -> None:
    """Fail loudly if the resolved stack is not the pinned one."""
    import pyspark
    assert pyspark.__version__ == "3.5.3", f"pyspark {pyspark.__version__} != 3.5.3"
    jars = sedona.sparkContext.getConf().get("spark.jars.packages", "")
    assert "sedona-spark-shaded-3.5_2.12:1.9.1" in jars, jars
    assert "geotools-wrapper:1.9.1-33.5" in jars, jars
    ver = sys.version_info
    assert (ver.major, ver.minor) == (3, 11), f"python {ver.major}.{ver.minor} != 3.11"
