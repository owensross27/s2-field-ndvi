# Run 6 artifacts (2026-08-08/09, us-west-2)

Evidence behind the "Run 6" section of `docs/spark-notes.md`. Logs are gzipped
Spark output, read with `gunzip -c <file>.gz | less`.

| File | What it is |
|---|---|
| `chain.log.gz` | marker summary of the engine head-to-head (start/OK per arm) |
| `arm-jiffle.log.gz` | jiffle arm, 150s / 2 scenes / 13,369 rows |
| `arm-python_udf.log.gz` | python_udf arm, 148s / 2 scenes / 13,369 rows |
| `arm-python_udf_tiled.log.gz` | python_udf at 128px tiles, 228s / 2 scenes / 13,369 rows |
| `prep.log.gz` | 01_fields + 02_scenes at mvp scope (41 scenes selected) |
| `run6-row1-attempt3-24g.log.gz` | mvp batch row: driver heap OOM, exit 137 at 72 min |
| `run6-row2.log.gz` | mvp per_scene row: healthy, zero spill, no scene done in 100 min |

Verify a claimed driver heap in any arm log with
`gunzip -c arm-jiffle.log.gz | grep "MemoryStore started"` — the second JVM in each
arm log is the run itself (14.2 GiB capacity = the 24g heap); the first is the
throwaway table-drop step at Spark's 1g default.
