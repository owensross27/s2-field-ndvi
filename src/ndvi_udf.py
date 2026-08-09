"""The one raster UDF: SCL cloud mask + NDVI + nodata (python_udf engine).

Inputs are SedonaRaster objects: 10m red/nir plus SCL already reproject-matched
onto the same 10m grid. Band values arrive ALREADY as surface reflectance — the
GeoTools read path applies the COG's internal scale/offset tags (verified
empirically), so no manual scaling here; the manifest's scale/offset columns are
provenance, checked for uniformity by the DQ stage.

Output is a single-band float64 raster with NODATA where masked, so downstream
RS_ZonalStatsAll(excludeNoData=true) drops cloudy pixels from mean and count.

as_numpy_masked() (never as_numpy) turns each band's NODATA into NaN: a hole in
both inputs must not cancel into a plausible zero.

NOTE macOS: this engine dies under load in local mode (JVM->worker socket writes
hit kernel ENOBUFS with multi-MB raster rows). Use ndvi_engine: jiffle on
laptops; this path is for Linux/EKS where it runs fine.
"""
import numpy as np
from pyspark.sql.functions import udf

from sedona.spark import RasterType

from config import QUALITY

NODATA = -9999.0
_MASK_CLASSES = np.array(QUALITY["scl_mask_classes"], dtype=np.float64)


@udf(returnType=RasterType())
def ndvi_masked(red, nir, scl):
    r = red.as_numpy_masked()[0].astype(np.float64)
    n = nir.as_numpy_masked()[0].astype(np.float64)
    s = scl.as_numpy_masked()[0]

    denom = n + r
    with np.errstate(invalid="ignore", divide="ignore"):
        ndvi = (n - r) / denom

    bad = (
        np.isnan(r) | np.isnan(n)
        | np.isin(s, _MASK_CLASSES) | np.isnan(s)
        # <= 0 and negative-band guards keep NDVI physical: L2A's BOA offset
        # produces small negative reflectances in dark pixels (same guard as
        # the jiffle script in 03 -- keep the two engines in exact parity)
        | (denom <= 0.0) | (r < 0.0) | (n < 0.0)
    )
    ndvi = np.where(bad, NODATA, ndvi)
    return red.with_bands(ndvi[np.newaxis, :, :], nodata=NODATA)
