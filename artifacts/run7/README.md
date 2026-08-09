# Run 7 artifacts (2026-08-09, m7g.4xlarge us-west-2)

Evidence behind spark-notes "Run 7". `chain.log.gz` has step markers;
`mvp-ndvi-perscene.log.gz` has the five measured mvp per-scene completions
(954-1253s each) that were impossible before the tile-grid equi-join
(run 2: zero scenes in 100 minutes on twice the cores). The partial mvp
warehouse (5/41 partitions, valid Iceberg snapshot) is banked at
s3://s2fn-run6-384555717200/run7/wh-mvp.tar.gz for an idempotent resume.
Phase-0 EXPLAIN evidence lives in ../run6/phase0-explain.txt.gz.
