# Run 9 — DiD at mvp scope: the pre-registered gate fired (2026-08-09, $0, local)

The event-study notebook re-ran unchanged against the merged mvp warehouse and
its monotonicity assert failed: DiD by wind band -0.031 / -0.093 / -0.069
(80-99 worse than 100+, ~9 SE apart). Forensics in `docs/spark-notes.md` Run 9;
narrative in the README section "Scaling the study 17x".

Files:

- `notebook-assert-failure.log` — the assert firing during headless execution
- `did_diag.py` + `did-diag.log.gz` — attrition by band, per-tile usable
  fractions on the event pair, raw deltas, DiD with SE, longitude-tercile
  decomposition (the confound), stricter-validity sensitivity
- `did_diag2.py` + `did-diag2.log.gz` — post-hoc lat+lon matched DiD
  (monotonicity restored) and the exact Benton-county reproduction of the
  demo-scope table from the mvp warehouse (data exonerated)

Repro (read-only against the canonical warehouse; never copy it):

```
docker run --rm -u 0 \
  -e PYTHONPATH=/opt/s2fn/src:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  -v $PWD/wh-mvp-final:/opt/s2fn/warehouse:ro \
  -v $PWD/src:/opt/s2fn/src:ro -v $PWD/config.yml:/opt/s2fn/config.yml:ro \
  -v $PWD/artifacts/run9:/opt/diag \
  s2-field-ndvi:latest python3.11 /opt/diag/did_diag.py
```

The PYTHONPATH line matters: the image ships no pip pyspark (spark-submit
injects the zips at launch); a directly-launched python — a notebook kernel or
these scripts — must add them itself.
