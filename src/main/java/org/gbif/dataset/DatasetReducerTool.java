/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.dataset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.rand;

import java.io.Serializable;
import java.util.Objects;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.storage.StorageLevel;

/**
 * Reduces a star-schema dataset made of 3 tab-separated files linked by eventID:
 *
 * <ul>
 *   <li>event.txt: the core file, one row per event
 *   <li>occurrence.txt: the biggest file, many rows per event
 *   <li>humboldt.txt: one row per event (1 to 1 with events)
 * </ul>
 *
 * <p>Every event that is kept has at least 1 occurrence, and the 3 output files stay consistent
 * with each other: no occurrence or humboldt row points to an event that is not in the output.
 */
@Slf4j
@Builder(toBuilder = true)
public class DatasetReducerTool implements Serializable {

  private static final String EVENT_ID = "eventID";
  private static final String RANDOM = "_random";
  private static final String MIN_RANDOM = "_min_random";

  private static final String OCCURRENCE = "occurrence";
  private static final String HUMBOLDT = "humboldt";
  private static final String EVENT = "event";

  private final String inputDir;
  private final String outputDir;
  private final double occurrenceFraction;
  private final double eventFraction;
  private final long seed;
  private final int numFiles;
  private final boolean inputDirContainsSubDirs;

  public static void main(String[] args) {
    DatasetReducerTool.builder()
        .inputDir(args[0])
        .outputDir(args[1])
        .occurrenceFraction(args.length > 2 ? Double.parseDouble(args[2]) : 0.0)
        .eventFraction(args.length > 3 ? Double.parseDouble(args[3]) : 1.0)
        .seed(args.length > 4 ? Long.parseLong(args[4]) : 42L)
        .numFiles(args.length > 5 ? Integer.parseInt(args[5]) : 1)
        .inputDirContainsSubDirs(args.length > 6 && Boolean.parseBoolean(args[6]))
        .build()
        .run();
  }

  @SneakyThrows
  public void run() {
    Objects.requireNonNull(inputDir, "inputDir is null");
    Objects.requireNonNull(outputDir, "outputDir is null");
    if (inputDir.equals(outputDir)) {
      throw new IllegalArgumentException("outputDir must be different from inputDir");
    }
    if (occurrenceFraction < 0 || occurrenceFraction > 1) {
      throw new IllegalArgumentException("occurrenceFraction must be between 0 and 1");
    }
    if (eventFraction <= 0 || eventFraction > 1) {
      throw new IllegalArgumentException("eventFraction must be in (0, 1]");
    }
    if (numFiles < 1) {
      throw new IllegalArgumentException("numFiles must be at least 1");
    }

    try (SparkSession spark =
        SparkSession.builder().appName("Dataset reducer tool").getOrCreate()) {

      // 1. events: optionally keep only a random fraction of them
      spark.sparkContext().setJobGroup(EVENT, "Read and sample events", false);
      Dataset<Row> events = readTsv(spark, EVENT);
      if (eventFraction < 1.0) {
        events = events.sample(false, eventFraction, seed);
      }
      events = events.persist(StorageLevel.DISK_ONLY());

      // 2. occurrences: only those of the kept events, and from each event:
      //    - the occurrence with the lowest random value (guarantees at least 1 per event)
      //    - plus every occurrence whose random value is below occurrenceFraction
      spark.sparkContext().setJobGroup(OCCURRENCE, "Reduce occurrences", false);
      WindowSpec perEvent = Window.partitionBy(EVENT_ID);
      Dataset<Row> keptOccurrences =
          semiJoin(readTsv(spark, OCCURRENCE), events)
              .withColumn(RANDOM, rand(seed))
              .withColumn(MIN_RANDOM, min(col(RANDOM)).over(perEvent))
              .filter(
                  col(RANDOM).equalTo(col(MIN_RANDOM)).or(col(RANDOM).lt(lit(occurrenceFraction))))
              .drop(RANDOM, MIN_RANDOM)
              .persist(StorageLevel.DISK_ONLY());

      writeTsv(spark, keptOccurrences, OCCURRENCE);
      long occurrenceCount = keptOccurrences.count(); // already cached by the write

      // 3. events: only those that ended up with at least 1 occurrence
      spark.sparkContext().setJobGroup("events-write", "Write events", false);
      Dataset<Row> keptEvents = semiJoin(events, keptOccurrences).persist(StorageLevel.DISK_ONLY());
      writeTsv(spark, keptEvents, EVENT);
      long eventCount = keptEvents.count();

      // 4. humboldt: only the rows of the kept events
      spark.sparkContext().setJobGroup(HUMBOLDT, "Reduce and write humboldt", false);
      Dataset<Row> keptHumboldt =
          semiJoin(readTsv(spark, HUMBOLDT), keptEvents).persist(StorageLevel.DISK_ONLY());
      writeTsv(spark, keptHumboldt, HUMBOLDT);
      long humboldtCount = keptHumboldt.count();

      log.info(
          "Reduced dataset written to {}: {} events, {} occurrences, {} humboldt rows",
          outputDir,
          eventCount,
          occurrenceCount,
          humboldtCount);

      keptHumboldt.unpersist();
      keptEvents.unpersist();
      keptOccurrences.unpersist();
      events.unpersist();
    }
  }

  /** Keeps only the rows of df whose eventID is present in keys. */
  private static Dataset<Row> semiJoin(Dataset<Row> df, Dataset<Row> keys) {
    Dataset<Row> k = keys.select(EVENT_ID).distinct().alias("k");
    return df.join(k, df.col(EVENT_ID).equalTo(col("k." + EVENT_ID)), "left_semi");
  }

  /** All columns are read as strings so the values are written back exactly as they were. */
  private Dataset<Row> readTsv(SparkSession spark, String fileType) {
    String inputPath = inputDirContainsSubDirs ? inputDir + "/" + fileType : inputDir;

    return spark
        .read()
        .option("header", "true")
        .option("delimiter", "\t")
        .option("inferSchema", "false")
        .option("quote", "") // the text files are not quoted: avoid misreading a leading '"'
        .csv(new Path(inputPath, fileType + ".txt").toString());
  }

  /**
   * Writes the dataset as outputDir/{name}.txt. With numFiles = 1 it is a single file, otherwise a
   * directory with numFiles part files (each one with its own header).
   */
  @SneakyThrows
  private void writeTsv(SparkSession spark, Dataset<Row> df, String name) {
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    Path tmp = new Path(outputDir, "_tmp_" + name);
    Path target = new Path(outputDir, name + ".txt");
    FileSystem fs = target.getFileSystem(conf);
    fs.delete(tmp, true);
    fs.delete(target, true);

    df.repartition(numFiles)
        .write()
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .option("delimiter", "\t")
        .option("quote", "")
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")
        .csv(tmp.toString());

    if (numFiles == 1) {
      FileStatus[] parts = fs.globStatus(new Path(tmp, "part-*"));
      fs.rename(parts[0].getPath(), target);
      fs.delete(tmp, true);
    } else {
      fs.rename(tmp, target);
    }
  }
}
