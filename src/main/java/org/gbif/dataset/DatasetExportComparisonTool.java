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

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_compact;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.when;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

@Builder(toBuilder = true)
public class DatasetExportComparisonTool implements Serializable {

  private static final String EXPORT_TABLE_ALIAS = "export";
  private static final String PROD_TABLE_ALIAS = "prod";

  private final String hiveDB;
  private final String sourceTable;
  private final String csvFilePath;
  private final String destinationTable;
  private final String datasetKey;

  public static void main(String[] args) {
    DatasetExportComparisonTool.builder()
        .hiveDB(args[0])
        .datasetKey(args[1])
        .sourceTable(args[2])
        .destinationTable(args[3])
        .csvFilePath(args.length > 4 ? args[4] : null)
        .build()
        .run();
  }

  @SneakyThrows
  public void run() {
    Objects.requireNonNull(hiveDB, "hiveDB is null");
    Objects.requireNonNull(sourceTable, "sourceTable is null");

    try (SparkSession spark =
        SparkSession.builder()
            .appName("Dataset export comparison tool")
            .config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath())
            .enableHiveSupport()
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .getOrCreate()) {
      spark.sql("use " + hiveDB);
      spark.sparkContext().conf().set("hive.exec.compress.output", "false");

      Dataset<Row> exportTable;
      if (csvFilePath != null && !csvFilePath.isEmpty()) {
        spark.sparkContext().setJobGroup("read-export", "Read export from CSV", false);
        exportTable =
            spark
                .read()
                .option("header", "true")
                .option("delimiter", "\t")
                .option("inferSchema", "false")
                .csv(csvFilePath);
      } else {
        spark.sparkContext().setJobGroup("read-export", "Read export from table", false);
        exportTable = spark.table(sourceTable);
      }

      spark.sparkContext().setJobGroup("read-prod", "Read dataset from prod table", false);
      Dataset<Row> prodDataset =
          spark.table("iceberg.prod_b.occurrence").filter(col("datasetkey").equalTo(datasetKey));
      Dataset<Row> prodDatasetVerbatim =
          prodDataset.select(col("gbifid"), prodDataset.colRegex("`v_.*`"));

      // stats
      exportTable = exportTable.persist(StorageLevel.DISK_ONLY());
      long exportCount = exportTable.count();
      long prodCount = prodDataset.count();

      Column prodKey = col(PROD_TABLE_ALIAS + ".v_occurrenceid");
      Column exportKey = col(EXPORT_TABLE_ALIAS + ".occurrenceid");

      spark.sparkContext().setJobGroup("join", "Join tables", false);
      Dataset<Row> joined =
          exportTable
              .alias(EXPORT_TABLE_ALIAS)
              .join(
                  prodDatasetVerbatim.alias(PROD_TABLE_ALIAS),
                  exportKey.equalTo(prodKey),
                  "full_outer");

      List<Column> selectedColumns = new ArrayList<>();
      for (String columnName : exportTable.columns()) {
        selectedColumns.add(
            col(EXPORT_TABLE_ALIAS + "." + columnName)
                .alias(EXPORT_TABLE_ALIAS + "_" + columnName));
      }
      for (String columnName : prodDatasetVerbatim.columns()) {
        selectedColumns.add(
            col(PROD_TABLE_ALIAS + "." + columnName).alias(PROD_TABLE_ALIAS + "_" + columnName));
      }

      // diffs
      Set<String> prodCols =
          Arrays.stream(prodDatasetVerbatim.columns())
              .map(String::toLowerCase)
              .collect(Collectors.toSet());

      List<Column> diffFlags = new ArrayList<>();
      for (String c : exportTable.columns()) {
        if (prodCols.contains("v_" + c.toLowerCase())) {
          Column r = nullify(normalize(col(EXPORT_TABLE_ALIAS + "." + c)));
          Column p = nullify(normalize(col(PROD_TABLE_ALIAS + ".v_" + c)));
          diffFlags.add(when(r.eqNullSafe(p), lit(null)).otherwise(lit(c)));
        }
      }
      Column diffArray = array_compact(array(diffFlags.toArray(Column[]::new)));
      Column matched = exportKey.equalTo(prodKey);

      selectedColumns.add(
          when(matched, lit("MATCH"))
              .when(exportKey.isNotNull(), lit("ONLY_EXPORT"))
              .otherwise(lit("ONLY_PROD"))
              .alias("match_status"));
      selectedColumns.add(when(matched, diffArray).alias("diff_columns"));

      dropTable(spark, destinationTable);

      spark.sparkContext().setJobGroup("write", "Save comparison table", false);
      Dataset<Row> result =
          joined
              .select(selectedColumns.toArray(Column[]::new))
              .withColumn(
                  "has_differences",
                  when(col("diff_columns").isNotNull(), size(col("diff_columns")).gt(0)));
      result
          .write()
          .format("parquet")
          .partitionBy("match_status")
          .mode(SaveMode.Overwrite)
          .saveAsTable(destinationTable);

      // stats
      Dataset<Row> written = spark.table(destinationTable);

      dropTable(spark, destinationTable + "_stats");
      Dataset<Row> perStatus = written.groupBy("match_status").count();
      Dataset<Row> totals =
          spark.createDataFrame(
              Arrays.asList(
                  RowFactory.create("EXPORT_TOTAL", exportCount),
                  RowFactory.create("PROD_TOTAL", prodCount)),
              perStatus.schema());

      spark.sparkContext().setJobGroup("write", "Write stats", false);
      perStatus
          .union(totals)
          .write()
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .saveAsTable(destinationTable + "_stats");

      // diffs
      dropTable(spark, destinationTable + "_column_diffs");
      spark.sparkContext().setJobGroup("write", "Write diffs", false);
      written
          .filter("has_differences = true")
          .select(explode(col("diff_columns")).alias("column"))
          .groupBy("column")
          .count()
          .write()
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .saveAsTable(destinationTable + "_column_diffs");
    }
  }

  @SneakyThrows
  private void dropTable(SparkSession spark, String tableName) {
    spark.sql("DROP TABLE IF EXISTS " + tableName + " PURGE");

    org.apache.hadoop.fs.Path path =
        new org.apache.hadoop.fs.Path("/stackable/warehouse/" + hiveDB + ".db/" + tableName);
    org.apache.hadoop.fs.FileSystem fs =
        path.getFileSystem(spark.sparkContext().hadoopConfiguration());
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
  }

  // lower-case and drop whitespace, underscores and pipes
  private static Column normalize(Column c) {
    return regexp_replace(lower(c), "[\\s_|]+", "");
  }

  // empty, blank or the literal "NULL" all become a real null
  private static Column nullify(Column c) {
    return when(upper(trim(c)).isin("", "NULL"), lit(null)).otherwise(c);
  }
}
