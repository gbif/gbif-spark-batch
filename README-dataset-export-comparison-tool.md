# DatasetExportComparisonTool

Spark job that compares a **dataset export** (for example the raw eBird file) with what is stored in GBIF's **production occurrence table**, record by record. It tells you which records are only in the export, which are only in production, and, for the records present in both, which fields differ.

Main class: `org.gbif.dataset.DatasetExportComparisonTool`

## What it does

1. **Reads the export.** From a tab-separated CSV file (with header) or from a Hive table. When read from CSV, all columns are strings (`inferSchema=false`).
2. **Reads production.** `iceberg.prod_b.occurrence`, filtered by `datasetkey`. It keeps `gbifid` and all verbatim columns (`v_*`).
3. **Counts both sides** (total export rows and total production rows for the dataset). The export is persisted to disk (`DISK_ONLY`) so it is only parsed once.
4. **Full outer join** on `export.occurrenceid = prod.v_occurrenceid`.
5. **Classifies every row** with a `match_status`:

   | match_status  | Meaning                                                  |
   |---------------|----------------------------------------------------------|
   | `MATCH`       | The `occurrenceid` is in both the export and production  |
   | `ONLY_EXPORT` | In the export but missing in production                  |
   | `ONLY_PROD`   | In production but missing in the export                  |

6. **Compares fields** for `MATCH` rows. Every export column `x` is compared with the production column `v_x`. Export columns without a production counterpart are not compared (they are still written to the output).
7. **Writes the results** as Parquet tables in the Hive database (see [Results](#results)).

### Comparison rules

Before two values are compared, both sides are normalized:

- lower-cased,
- whitespace, underscores (`_`) and pipes (`|`) are removed,
- an empty value or the literal text `NULL` is treated as a null.

Then the values are compared with null-safe equality (null equals null). For example, these are **not** reported as differences:

| Export value        | Production value |
|---------------------|------------------|
| `HUMAN_OBSERVATION` | `HumanObservation` |
| *(empty)*           | `NULL`           |

The normalization is only used for the comparison. The values stored in the output tables are the original ones. It applies to all compared columns. Any other difference (hyphens, dots, accents, number formats...) is reported as a difference.

## Parameters

The job takes positional arguments:

| # | Name               | Required | Description |
|---|--------------------|----------|-------------|
| 0 | `hiveDB`           | yes      | Hive database where the results are written. The job runs `USE <hiveDB>`. |
| 1 | `datasetKey`       | yes      | GBIF dataset key (UUID) used to filter the production table. |
| 2 | `sourceTable`      | yes      | Table with the export, read when no CSV path is given. The argument is required even when a CSV is used (in that case its value is ignored). |
| 3 | `destinationTable` | yes      | Name of the main results table. |
| 4 | `csvFilePath`      | no       | Path to a tab-separated file with a header. When set, the file is read instead of `sourceTable`. |

## Running

Requirements:

- Spark with Hive support and an Iceberg catalog named `iceberg` (configured by the job as a Hive catalog), with the Iceberg runtime on the classpath.
- Read access to `iceberg.prod_b.occurrence`.
- Write access to `/stackable/warehouse/<hiveDB>.db/`. The job uses this location to clean up old tables.

Reading the export from a Hive table:

```bash
spark-submit \
  --class org.gbif.dataset.DatasetExportComparisonTool \
  <application-jar> \
  <hiveDB> <datasetKey> <sourceTable> <destinationTable>
```

Reading the export from a CSV file:

```bash
spark-submit \
  --class org.gbif.dataset.DatasetExportComparisonTool \
  <application-jar> \
  <hiveDB> <datasetKey> <anyValue> <destinationTable> <csvFilePath>
```

Example (eBird):

```bash
spark-submit \
  --class org.gbif.dataset.DatasetExportComparisonTool \
  <application-jar> \
  my_db 4fa7b334-ce0d-4e88-aaae-2e0c138d049e ebird_raw ebird_2025_comparison
```

You can find an example to run it in K8s in the [gbif-configuration](https://github.com/gbif/gbif-configuration/blob/7561b1507e27d479115cc2db73a93b62d354a533/dataset_export_tool/ebird_prod_spark_job_template_fast_storage.yaml) repo.

## Results

Each run drops and recreates the following tables in `<hiveDB>` (`<dest>` is the `destinationTable` value). All are Parquet.

### `<dest>`: the comparison table, one row per record

Partitioned by `match_status`, so a query that filters by it only reads the matching directory.

| Column(s)          | Content                                                                                                                                                           |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `export_<column>`  | Every column of the export, with the `export_` prefix. Null for `ONLY_PROD` rows.                                                                                 |
| `prod_gbifid`, `prod_v_<column>` | `gbifid` and every verbatim column of production, with the `prod_` prefix. Null for `ONLY_EXPORT` rows.                                                           |
| `diff_columns`     | For `MATCH` rows, an array of the columns that differ, e.g. `locality,eventdate`. Empty if all compared columns are equal. Null for the other statuses.    |
| `has_differences`  | For `MATCH` rows, `true` if `diff_columns` is not empty. Null for the other statuses.                                                                             |
| `match_status`     | `MATCH`, `ONLY_EXPORT` or `ONLY_PROD`. It is the partition column, so Spark stores it in the directory name (`match_status=MATCH`), not inside the Parquet files. |

### `<dest>_stats`: totals

Columns: `match_status` (string), `count` (bigint).

| match_status   | count |
|----------------|-------|
| `MATCH`        | rows in both |
| `ONLY_EXPORT`  | rows only in the export |
| `ONLY_PROD`    | rows only in production |
| `EXPORT_TOTAL` | total rows in the export |
| `PROD_TOTAL`   | total rows in production for the dataset |

A status without rows does not appear. If the keys are unique, `MATCH + ONLY_EXPORT = EXPORT_TOTAL` and `MATCH + ONLY_PROD = PROD_TOTAL`. If a sum is larger, the join produced extra rows because of duplicate `occurrenceid` values.

### `<dest>_column_diffs`: which fields differ

Columns: `column`, `count`. For every column, the number of `MATCH` records where it differs. A record with several differing fields counts once per field, so these counts are not a number of records. Use `<dest>_stats` and `has_differences` for record totals.

| column           | count |
|------------------|-------|
| `scientificname` | 14608737 |
| `taxonconceptid` | 1417196599 |
| `taxonrank`      | 97460 |

## Querying the results

```sql
-- totals
SELECT * FROM <dest>_stats;

-- most frequent field differences
SELECT * FROM <dest>_column_diffs ORDER BY count DESC;

-- matched records with differences, and the fields involved
SELECT export_occurrenceid, prod_gbifid, diff_columns
FROM <dest>
WHERE match_status = 'MATCH' AND has_differences;

-- records missing in production
SELECT export_occurrenceid FROM <dest> WHERE match_status = 'ONLY_EXPORT';

-- records in production that are not in the export
SELECT prod_gbifid, prod_v_occurrenceid FROM <dest> WHERE match_status = 'ONLY_PROD';

-- difference between 2 columns
select export_taxonconceptid, prod_v_taxonconceptid from ebird_2025_v2_comparison where match_status = 'MATCH' and has_differences = true and array_contains(diff_columns, 'taxonconceptid');


select export_taxonconceptid, prod_v_taxonconceptid, diff_columns from ebird_2025_v2_comparison where match_status = 'MATCH' and has_differences = true and contains(diff_columns, 'taxonconceptid') limit 5;

export_taxonconceptid |  prod_v_taxonconceptid   |                                  diff_columns
-----------------------+--------------------------+--------------------------------------------------------------------------------
 avibase-D77E4B41      | avibase-avibase-D77E4B41 | [eventid, taxonconceptid, genericname, taxonrank, taxonomicstatus]
 avibase-0783A7EA      | avibase-avibase-0783A7EA | [eventid, taxonconceptid, genericname, taxonrank, taxonomicstatus]
 avibase-4E74AE22      | avibase-avibase-4E74AE22 | [eventid, taxonconceptid, genericname, taxonrank, taxonomicstatus]
 avibase-B745D852      | avibase-avibase-B745D852 | [eventid, taxonconceptid, genericname, taxonrank, taxonomicstatus]
 avibase-23863F65      | avibase-avibase-23863F65 | [eventid, recordedby, taxonconceptid, genericname, taxonrank, taxonomicstatus]

```

## Sharing a subset of the results

Each status is a separate directory under the table location, so you can copy only the one you need:

```bash
hdfs dfs -get '<table-location>/match_status=ONLY_PROD/*.parquet' ./only_prod/
```

Recipients can open the files with DuckDB:

```sql
SELECT * FROM read_parquet('only_prod/*.parquet');
```

The `match_status` column is not inside the files. If someone copies the whole table directory, DuckDB can recover it with `read_parquet('<dir>/*/*.parquet', hive_partitioning = true)`.

The files keep the full schema: `ONLY_PROD` files include all the `export_*` columns (all null), and `ONLY_EXPORT` files include all the `prod_*` columns. Select only the columns of the side you need, for example `SELECT COLUMNS('^prod_') FROM ...` in DuckDB.

To find the table location in Spark use `DESCRIBE FORMATTED <dest>`, and in Trino `SELECT DISTINCT "$path" FROM hive.<hiveDB>.<dest>`.

## Notes and limitations

- **Results are replaced on every run.** The four tables (and their directories under `/stackable/warehouse/<hiveDB>.db/`) are dropped at the start. Copy out anything you need to keep.
- **Duplicate keys.** If `occurrenceid` is repeated on either side, the join multiplies rows. Compare the totals as described under `<dest>_stats` to detect it.
- **Null keys.** A row of the export with a null `occurrenceid` never matches and is classified as `ONLY_PROD`.
- **Only same-name columns are compared.** Export column `x` is compared with `v_x`.
- **Everything is a string when reading from CSV.** With a Hive table, the comparison uses whatever types the table has.
- **Hard-coded names.** The production table is `iceberg.prod_b.occurrence`, the join key is `occurrenceid` / `v_occurrenceid` and the warehouse path is `/stackable/warehouse`.
