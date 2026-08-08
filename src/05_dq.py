"""Great Expectations data-quality gate over local.crop.scenes + field_ndvi.

Hard checks (any failure -> nonzero exit; runs as the last step of
`make pipeline` and standalone as `make dq`): scene coverage per
(season, tile), NDVI range, valid_frac range, no duplicate
(field_id, date, mgrs_tile), field_ndvi row count > 0. Every check result —
GX and plain-python — lands as one row in local.crop.dq_results. GX Data
Docs (HTML) render to web/dq/.

ponytail: GX pandas engine over toPandas() holds through mvp scope (~4.5M
rows); state scale needs the GX Spark engine or DuckDB over the GeoParquet
drop (data/publish/field_ndvi.parquet).
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DQ, ICEBERG, REPO_ROOT, scope
from session import assert_versions, get_sedona

CAT = ICEBERG["catalog"]
DOCS_DIR = REPO_ROOT / "web" / "dq"


def check_row(name: str, scope_name: str, expected: str, observed: str, passed: bool) -> dict:
    return dict(check_name=name, scope=scope_name, expected=expected, observed=str(observed),
                passed=bool(passed), run_at=datetime.now(timezone.utc))


def observed_str(result: dict) -> str:
    """GX ExpectationValidationResult.result dict -> one-line observed value."""
    if "observed_value" in result:
        return str(result["observed_value"])
    return f"{result.get('unexpected_count', 0)} of {result.get('element_count', 0)} rows failed"


def scene_coverage_pdf(scenes_pdf: pd.DataFrame, sc: dict) -> pd.DataFrame:
    """One row per (season, tile) the scope expects, with its selected-scene count.

    ponytail: tiles == "all" (state/history scopes) -- the STAC grid sweep
    that resolves "all" isn't available here, so the expected tile universe
    falls back to whatever tiles the run actually touched for ANY season.
    Still catches a season-wide gap like c1-l2a 2022 (that season vanishes
    from every tile, not just one), but a tile that is ENTIRELY missing from
    every season can't be detected this way -- upgrade path is resolving the
    tile universe from the same STAC grid:code sweep 02_scenes.py uses, or
    pinning the 29-tile state list in config.yml.
    """
    tiles = sc["tiles"] if sc["tiles"] != "all" else sorted(scenes_pdf["mgrs_tile"].unique())
    expected = pd.DataFrame([(s, t) for s in sc["seasons"] for t in tiles],
                             columns=["season", "mgrs_tile"])
    counts = (scenes_pdf[scenes_pdf["selected"]]
              .groupby(["season", "mgrs_tile"]).size()
              .rename("n_selected").reset_index())
    merged = expected.merge(counts, "left", on=["season", "mgrs_tile"])
    merged["n_selected"] = merged["n_selected"].fillna(0).astype(int)
    return merged


def scoped_table(sedona, table: str, sc: dict) -> pd.DataFrame:
    """Read a crop table filtered to this run's scope, not the whole table --
    otherwise SCOPE=demo validates (and can be hard-failed by) rows an
    mvp/state run already wrote elsewhere in the same warehouse."""
    df = sedona.table(f"{CAT}.crop.{table}")
    if sc["tiles"] != "all":
        df = df.filter(F.col("mgrs_tile").isin(sc["tiles"]))
    df = df.filter(F.col("season").isin(sc["seasons"]))
    if "dates" in sc:
        df = df.filter(F.col("date").isin(sc["dates"]))
    return df.toPandas()


def write_rows(sedona, rows: list[dict]) -> None:
    sedona.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.crop")
    # explicit DDL schema: pyspark's dict-row inference sorts columns
    # alphabetically (pyspark 3.5.3 _infer_schema), which would silently
    # scramble the on-disk column order and warn on every run
    dq_schema = ("check_name string, scope string, expected string, "
                 "observed string, passed boolean, run_at timestamp")
    writer = (sedona.createDataFrame(rows, schema=dq_schema)
              .coalesce(1).writeTo(f"{CAT}.crop.dq_results"))
    if sedona.catalog.tableExists(f"{CAT}.crop.dq_results"):
        writer.append()
    else:
        writer.create()


def main() -> None:
    sc = scope()
    sedona = get_sedona("05_dq")
    assert_versions(sedona)

    scenes_pdf = scoped_table(sedona, "scenes", sc)
    ndvi_pdf = scoped_table(sedona, "field_ndvi", sc)

    ctx = gx.get_context(mode="ephemeral")
    ctx.update_data_docs_site(site_name="local_site", site_config={
        "class_name": "SiteBuilder",
        "store_backend": {"class_name": "TupleFilesystemStoreBackend",
                           "base_directory": str(DOCS_DIR)},
        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    })
    ds = ctx.data_sources.add_pandas("pandas_ds")

    cov = scene_coverage_pdf(scenes_pdf, sc)
    if cov.empty:
        # the emptiest possible failure still deserves an audit row — every
        # other outcome lands in dq_results, so must this one
        write_rows(sedona, [check_row(
            "scene_coverage_nonempty", sc["name"],
            ">= 1 (season, tile) pair in scope",
            "0 -- scenes table has no rows for this scope", False)])
        print("DQ FAILED: scenes table has no rows for this scope", file=sys.stderr)
        sys.exit(1)

    # (check_name, expected description, asset name, dataframe, expectation)
    checks = [
        ("scenes_per_season_tile_min",
         f">= {DQ['min_scenes_per_season_tile']} selected scenes per (season, tile)",
         "scene_coverage", cov,
         gxe.ExpectColumnValuesToBeBetween(
             column="n_selected", min_value=DQ["min_scenes_per_season_tile"])),
        ("field_ndvi_mean_ndvi_range", "in [-1.0, 1.0]", "field_ndvi", ndvi_pdf,
         gxe.ExpectColumnValuesToBeBetween(column="mean_ndvi", min_value=-1.0, max_value=1.0)),
        ("field_ndvi_valid_frac_range", "in [0.0, 1.0]", "field_ndvi", ndvi_pdf,
         gxe.ExpectColumnValuesToBeBetween(column="valid_frac", min_value=0.0, max_value=1.0)),
        ("field_ndvi_no_duplicate_field_date", "unique (field_id, date, mgrs_tile) pairs",
         "field_ndvi", ndvi_pdf,
         gxe.ExpectCompoundColumnsToBeUnique(column_list=["field_id", "date", "mgrs_tile"])),
        ("field_ndvi_row_count_gt_zero", ">= 1 row", "field_ndvi", ndvi_pdf,
         gxe.ExpectTableRowCountToBeBetween(min_value=1)),
    ]

    rows: list[dict] = []
    failed = False
    for i, (name, expected, asset_name, df, expectation) in enumerate(checks):
        asset = ds.add_dataframe_asset(f"{asset_name}_{i}")
        bd = asset.add_batch_definition_whole_dataframe("batch")
        suite = gx.ExpectationSuite(name=f"{name}_suite")
        suite.add_expectation(expectation)
        ctx.suites.add(suite)
        vd = ctx.validation_definitions.add(
            gx.ValidationDefinition(name=f"{name}_validation", data=bd, suite=suite))
        result = vd.run(batch_parameters={"dataframe": df}).results[0]
        passed = bool(result.success)
        failed = failed or not passed
        observed = observed_str(result.to_json_dict()["result"])
        if name == "scenes_per_season_tile_min" and not passed:
            # GX's observed_value/unexpected_count is just a tally -- name the
            # actual short (season, tile) pairs so a FAIL row is diagnosable.
            short = cov[cov["n_selected"] < DQ["min_scenes_per_season_tile"]]
            observed = "missing: " + ", ".join(
                f"{r.season}/{r.mgrs_tile}" for r in short.itertuples())
        rows.append(check_row(name, sc["name"], expected, observed, passed))
        print(f"[{'PASS' if passed else 'FAIL'}] {name}: expected {expected}"
              + ("" if passed else f", observed {observed}"))

    # Warn-only: row-count delta vs the PREVIOUS Iceberg snapshot, via time
    # travel over the .history metadata table. Plain python, not GX — GX 1.x
    # expectations validate one batch, they have no cross-snapshot concept.
    #
    # ponytail: this compares against the previous APPEND, not the previous
    # RUN of the pipeline -- 03_ndvi_zonal.py appends once per process_batch
    # call, so under raster.per_scene=true it appends once per SCENE, and
    # "previous snapshot" degrades to "the scene before this one" (trivially
    # >=). A correct fix needs a run boundary this script doesn't have
    # (a run-id or a timestamp passed in from the caller); not building that
    # for a check that only ever warns.
    history = sedona.sql(
        f"SELECT snapshot_id FROM {CAT}.crop.field_ndvi.history "
        "ORDER BY made_current_at DESC LIMIT 2").collect()
    cur_n = len(ndvi_pdf)
    if len(history) < 2:
        rows.append(check_row("field_ndvi_row_count_vs_previous_snapshot", sc["name"],
                               "previous snapshot", "none (first snapshot)", True))
        print("[WARN-SKIP] field_ndvi_row_count_vs_previous_snapshot: no previous snapshot")
    else:
        prev_id = history[1].snapshot_id
        prev_n = sedona.sql(
            f"SELECT count(*) AS n FROM {CAT}.crop.field_ndvi VERSION AS OF {prev_id}"
        ).collect()[0].n
        ok = cur_n >= prev_n
        rows.append(check_row("field_ndvi_row_count_vs_previous_snapshot", sc["name"],
                               f">= previous ({prev_n})", f"{cur_n} (delta {cur_n - prev_n:+d})", ok))
        print(f"[{'OK' if ok else 'WARN'}] field_ndvi_row_count_vs_previous_snapshot: "
              f"{cur_n} vs previous {prev_n} (never fails the run)")

    write_rows(sedona, rows)

    urls = ctx.build_data_docs()
    print(f"data docs: {urls.get('local_site', DOCS_DIR)}")

    if failed:
        print("DQ FAILED: one or more hard checks did not pass", file=sys.stderr)
        sys.exit(1)
    print("DQ passed: all hard checks green")


if __name__ == "__main__":
    main()
