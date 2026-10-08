# DatasetReducerTool

Spark job that makes a star-schema dataset smaller while keeping it consistent. It is meant for datasets that are too big to work with, made of three tab-separated files linked by `eventID`:

| File            | Content                                                   |
|-----------------|-----------------------------------------------------------|
| `event.txt`     | Core file, one row per event                              |
| `occurrence.txt` | The biggest file, many rows per event                     |
| `humboldt.txt`  | One row per event (1 to 1 relationship with `event.txt`) |

The reduced dataset has **at least one occurrence for every event**, and the three output files stay consistent with each other: no occurrence or humboldt row points to an event that is not in the output.

Main class: `org.gbif.dataset.DatasetReducerTool`

## What it does

1. **Reads and samples the events.** It reads `event.txt`. If `eventFraction` is lower than 1, it keeps only a random fraction of the events.
2. **Reduces the occurrences.** It keeps only the occurrences of the kept events. Every occurrence gets a random number between 0 and 1, and an occurrence is kept when:
   - it has the **lowest random number of its event**, which guarantees at least 1 occurrence per event, or
   - its random number is **below `occurrenceFraction`**, which keeps about that share of the occurrences.

   With `occurrenceFraction = 0`, exactly 1 occurrence per event is kept.
3. **Drops the events without occurrences.** Only the events that ended up with at least one occurrence are written.
4. **Reduces humboldt.** Only the rows of the kept events are written.
5. **Writes the three files** as `event.txt`, `occurrence.txt` and `humboldt.txt` in the output directory, and logs how many rows each one has.

All columns are read as strings and written back as they were, so the values are not changed. Empty values stay empty.

## Parameters

The job takes positional arguments. Only the first two are required.

| # | Name                 | Default | Description                                                                                          |
|---|----------------------|---------|------------------------------------------------------------------------------------------------------|
| 0 | `inputDir`           |         | Directory with `event.txt`, `occurrence.txt` and `humboldt.txt`. It can be an HDFS, S3 or local URI. |
| 1 | `outputDir`          |         | Directory where the reduced files are written. It must be different from `inputDir`.                 |
| 2 | `occurrenceFraction` | `0.0`   | Share of occurrences to keep **in addition to** the 1 per event. Between 0 and 1.                    |
| 3 | `eventFraction`      | `1.0`   | Share of events to keep. Greater than 0 and up to 1.                                                 |
| 4 | `seed`               | `42`    | Seed for the random choices. The same seed gives the same result on the same input.                  |
| 5 | `numFiles`           | `1`     | Number of part files of each output. See [Results](#results).                                        |
| 6 | `inputDirContainsSubDirs`           | `false` | True if the input files are under its own directory, e.g. occurrence/occurrence.txt.                 |

### Size of the result

| Goal                                               | Arguments |
|----------------------------------------------------|-----------|
| Smallest dataset: 1 occurrence per event           | `<in> <out>` |
| 1 occurrence per event plus about 1% of the rest   | `<in> <out> 0.01` |
| Half of the events, 1 occurrence each              | `<in> <out> 0 0.5` |
| Half of the events, with about 1% of their rest    | `<in> <out> 0.01 0.5` |

The number of occurrences in the output is a little below `events + occurrenceFraction * occurrences`, because the chosen occurrence of an event is often already below the fraction and is not counted twice.

## Running

Requirements:

- Spark 3.x. Hive and Iceberg are not needed.
- Read access to `inputDir` and write access to `outputDir`.
- Enough local disk on the executors: intermediate results are persisted with `DISK_ONLY`.

```bash
spark-submit \
  --class org.gbif.dataset.DatasetReducerTool \
  <application-jar> \
  <inputDir> <outputDir> [occurrenceFraction] [eventFraction] [seed] [numFiles]
```

Example: 1 occurrence per event plus about 1% of the rest, all events, one file per output:

```bash
spark-submit \
  --class org.gbif.dataset.DatasetReducerTool \
  <application-jar> \
  hdfs://<nameservice>/data/full hdfs://<nameservice>/data/small 0.01 1.0 42 1
```

A local path only works if the files exist at that path on every executor. On a cluster, use HDFS or S3.

You can find an example to run it in K8s in the [gbif-configuration](https://github.com/gbif/gbif-configuration/blob/7561b1507e27d479115cc2db73a93b62d354a533/dataset-batch-spark/ebird_reducer_prod_spark_job_template_fast_storage.yaml) repo.

## Results

The output directory contains:

| Path             | Content |
|------------------|---------|
| `event.txt`     | The kept events. Every one has at least 1 occurrence in `occurrence.txt`. |
| `occurrence.txt` | The kept occurrences. |
| `humboldt.txt`   | The humboldt rows of the kept events. |

All three keep the same columns, the tab delimiter and the header of the input files.

- **`numFiles = 1`:** each output is a single file, like the input.
- **`numFiles > 1`:** each output is a directory (for example `occurrence.txt/`) with that number of part files, and each part file has its own header. Spark and DuckDB read the directory as one dataset.
- **Temporary directories:** each file is first written to `_tmp_<name>` inside the output directory and then moved to its final name. They are removed when the job finishes.
- **Existing results are replaced.** If `event.txt`, `occurrence.txt` or `humboldt.txt` already exist in `outputDir`, they are deleted at the start of each write.

The job logs a final line with the counts, for example:

```
Reduced dataset written to <outputDir>: <n> events, <n> occurrences, <n> humboldt rows
```

The number of humboldt rows equals the number of events when the input is a strict 1 to 1 relationship.

## Notes and limitations

- **Events without occurrences are dropped.** If an event has no occurrence in the input, "at least 1 occurrence per event" cannot hold for it, so it is removed from `event.txt` and `humboldt.txt`. Compare the event count in the log with the input to see how many were removed.
- **Orphan rows are dropped.** Occurrences and humboldt rows whose `eventID` is not in `event.txt` (or is empty) are not written.
- **Reproducibility.** The same `seed` gives the same result as long as the input files keep the same partitioning. Change the seed to get a different sample.
- **No quoting.** The text files are read and written without quote characters, because they are normally not quoted. If your files quote fields, the quote options in `readTsv` and `writeTsv` need to be removed.
- **Skew.** The occurrences of an event are processed together in one task. It only matters if a single event has a huge number of occurrences.
- **The key column is `eventID`.** It must be present in the three files. The name is matched without regard to case.
- **`outputDir` check.** The job only rejects an `outputDir` that is textually identical to `inputDir`. Make sure the two locations do not overlap in any other way, since existing output files are deleted.
