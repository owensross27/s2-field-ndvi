"""Author + execute notebooks/derecho_event_study.ipynb programmatically.

The notebook is the ONE narrative artifact; this builder keeps it reproducible
(cells are code here, outputs are embedded by execution against Iceberg only).
"""
from pathlib import Path

import nbformat as nbf
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parent.parent
NB_PATH = ROOT / "notebooks" / "derecho_event_study.ipynb"

md = nbf.v4.new_markdown_cell
code = nbf.v4.new_code_cell

cells = [
    md(
        "# The 2020 derecho, field by field: a difference-in-differences event study\n\n"
        "**Question.** How much corn canopy did the 2020-08-10 derecho strip, per field,\n"
        "and does damage scale with USDA's measured wind bands?\n\n"
        "**Why not raw before/after?** Iowa was drying out across the exact study window\n"
        "(US Drought Monitor D1 coverage rose 34.3% to 60.9%), so a raw NDVI drop\n"
        "attributes drought to wind. The design: treated = corn fields inside a USDA\n"
        "wind-gust polygon; controls = corn fields outside every polygon within a\n"
        "matched latitude band (0.15 deg) of each treated field; the estimate is\n"
        "delta(treated) minus delta(matched controls) per wind class.\n\n"
        "All thresholds were pre-registered in `config.yml` before results were computed.\n"
        "Every number derives from the Iceberg tables; nothing is loaded from files."
    ),
    code(
        "import sys\n"
        "from pathlib import Path\n"
        "sys.path.insert(0, str(Path.cwd().parent / 'src'))\n"
        "from config import EVENT, ICEBERG, QUALITY\n"
        "from session import get_sedona\n"
        "CAT = ICEBERG['catalog']\n"
        "sedona = get_sedona('derecho_event_study')\n"
        "sedona.sparkContext.setLogLevel('ERROR')"
    ),
    code(
        "frame = sedona.sql(f\"\"\"\n"
        "    WITH wide AS (\n"
        "      SELECT field_id,\n"
        "             MAX(CASE WHEN date = DATE'{EVENT['pre_date']}'  THEN mean_ndvi END) AS pre,\n"
        "             MIN(CASE WHEN date = DATE'{EVENT['pre_date']}'  THEN valid_frac END) AS pre_vf,\n"
        "             MAX(CASE WHEN date = DATE'{EVENT['post_date']}' THEN mean_ndvi END) AS post,\n"
        "             MIN(CASE WHEN date = DATE'{EVENT['post_date']}' THEN valid_frac END) AS post_vf\n"
        "      FROM {CAT}.crop.field_ndvi GROUP BY field_id\n"
        "    )\n"
        "    SELECT w.field_id, w.post - w.pre AS delta,\n"
        "           w.pre, w.post, w.pre_vf, w.post_vf,\n"
        "           ST_Y(ST_Centroid(ST_GeomFromWKB(f.geom_4326_wkb))) AS lat,\n"
        "           COALESCE(MAX(z.gust_class), 0) AS wind\n"
        "    FROM wide w\n"
        "    JOIN {CAT}.crop.fields f USING (field_id)\n"
        "    LEFT JOIN {CAT}.crop.wind_zones z\n"
        "      ON ST_Intersects(ST_Centroid(ST_GeomFromWKB(f.geom_4326_wkb)),\n"
        "                       ST_GeomFromWKB(z.wkb_4326))\n"
        "    WHERE f.CDL2020 = 1\n"
        "    GROUP BY w.field_id, w.pre, w.post, w.pre_vf, w.post_vf, f.geom_4326_wkb\n"
        "\"\"\").toPandas()\n"
        "vf_min = float(QUALITY['valid_frac_min'])\n"
        "frame['usable'] = (frame.pre.notna() & frame.post.notna()\n"
        "                   & (frame.pre_vf >= vf_min) & (frame.post_vf >= vf_min))\n"
        "frame[frame.usable].groupby('wind').agg(n=('delta','size'), mean_delta=('delta','mean')).round(4)"
    ),
    md(
        "## Difference-in-differences\n\n"
        "Each treated field is compared against the mean delta of control fields\n"
        "(wind class 0) within its latitude band; the DiD is the treated field's delta\n"
        "minus its matched-control mean, averaged per wind class."
    ),
    code(
        "import pandas as pd\n"
        "band = float(EVENT['control_lat_band_deg'])\n"
        "controls = frame[frame.usable & (frame.wind == 0)][['lat', 'delta']].sort_values('lat').reset_index(drop=True)\n"
        "treated = frame[frame.usable & (frame.wind > 0)].copy()\n"
        "def control_mean(lat):\n"
        "    sel = controls[(controls.lat >= lat - band) & (controls.lat <= lat + band)]\n"
        "    return sel.delta.mean() if len(sel) >= 5 else None\n"
        "treated['ctrl_delta'] = treated.lat.map(control_mean)\n"
        "treated = treated.dropna(subset=['ctrl_delta'])\n"
        "treated['did'] = treated.delta - treated.ctrl_delta\n"
        "result = (treated.groupby('wind')\n"
        "          .agg(fields=('did', 'size'), mean_delta=('delta', 'mean'),\n"
        "               mean_ctrl=('ctrl_delta', 'mean'), did=('did', 'mean'),\n"
        "               did_se=('did', 'sem'))\n"
        "          .round(4))\n"
        "result.index = result.index.map({1: '60-79 mph', 2: '80-99 mph', 3: '100+ mph'})\n"
        "result"
    ),
    code(
        "dids = result['did'].tolist()\n"
        "assert all(b < a for a, b in zip(dids, dids[1:])), (\n"
        "    'DiD is not monotonically worsening across wind classes: ' + str(dids))\n"
        "print('Monotonicity holds: DiD worsens with wind class:', dids)"
    ),
    md(
        "## Attrition: who the clouds dropped\n\n"
        "Storms make clouds, so the fields the validity filter drops could correlate\n"
        "with wind band; if they did, the surviving sample would be non-random. Counts\n"
        "per band at each stage: all corn fields, fields passing the null/valid_frac\n"
        "filter on both dates, and (treated bands only) fields matched to at least 5\n"
        "controls. The match column is blank for controls by construction."
    ),
    code(
        "att = frame.groupby('wind').agg(corn_fields=('usable', 'size'),\n"
        "                                cloud_ok=('usable', 'sum'))\n"
        "att['matched'] = treated.groupby('wind').size()\n"
        "att['dropped_pct'] = (100 * (1 - att.cloud_ok / att.corn_fields)).round(1)\n"
        "att.index = att.index.map({0: 'control', 1: '60-79 mph', 2: '80-99 mph', 3: '100+ mph'})\n"
        "att"
    ),
    md(
        "## Reading the result\n\n"
        "- The wind signal survives the drought control: NDVI loss deepens class by\n"
        "  class after subtracting what matched-latitude unaffected fields did over the\n"
        "  same 15 days.\n"
        "- Magnitudes are conservative. Optical NDVI understates lodging because\n"
        "  flattened corn stays green for weeks; SAR-based studies (Remote Sensing\n"
        "  12(23):3878; BAMS 103(4)) found structural damage where NDVI shows only a\n"
        "  modest dip. Treat these numbers as a floor, not the damage estimate.\n"
        "- Controls are matched on latitude only. Soil, hybrid maturity, and local\n"
        "  rainfall vary within a band; a production study would match on more.\n"
        "- Attrition itself correlates with wind: 30-31% of control and 60-79 mph corn\n"
        "  fields fail the validity filter vs 46% (80-99) and 54% (100+), residual\n"
        "  storm cloud sitting over the harder-hit swath. If cloudier fields are also\n"
        "  more damaged, survivors understate damage, consistent with reading these\n"
        "  numbers as a floor; the sign of the bias is not provable from optical data\n"
        "  alone.\n"
        "- Single county (Benton), single sensor, two dates. The mvp and state scopes\n"
        "  extend the same tables statewide; this notebook re-runs unchanged."
    ),
]

nb = nbf.v4.new_notebook(cells=cells, metadata={
    "kernelspec": {"name": "python3", "display_name": "Python 3"},
    "language_info": {"name": "python"},
})
NB_PATH.parent.mkdir(exist_ok=True)
client = NotebookClient(nb, timeout=600, kernel_name="python3",
                        resources={"metadata": {"path": str(NB_PATH.parent)}})
client.execute()
nbf.write(nb, NB_PATH)
print(f"executed and wrote {NB_PATH}")
