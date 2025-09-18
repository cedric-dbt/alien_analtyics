Purpose
-------

This `models/ops` folder contains temporary, lightweight models used for debugging and validation during development. They are not meant to be part of the regular production build.

Files
-----
- `mart_row_counts.sql` — materializes a table with one row per mart and the row_count (used for quick verification)
- `sample_mart_ufo_top_locations.sql` — 100-row sample of `mart_ufo_top_locations`
- `sample_mart_ufo_sightings_by_country_time.sql` — 100-row sample of `mart_ufo_sightings_by_country_time`

How to run
----------
Run ops models on-demand:

```bash
# materialize ops models
dbt run --select tag:ops
# print counts using the helper macro
dbt run-operation print_ops_counts
```

How to exclude from CI
----------------------
Exclude ops from CI or scheduled builds by using the `--exclude` selector:

```bash
# run full build but skip ops
dbt build --exclude tag:ops
```

Cleanup
-------
When you no longer need the ops models, you can:

- Move the SQL into `analysis/` (they will no longer be built by default).
- Or remove the files from the repo.
