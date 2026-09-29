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
package org.gbif.ebird;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.when;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

@Builder(toBuilder = true)
public class EbirdComparisonTool implements Serializable {

  private static final String ID_PREFIX = "URN:catalog:CLO:EBIRD:";
  private static final String RAW_TABLE_ALIAS = "raw_table";
  private static final String PROD_TABLE_ALIAS = "prod_table";
  private static final String EBIRD_DATASET_KEY = "4fa7b334-ce0d-4e88-aaae-2e0c138d049e";
  private static final String COMPARISON_TABLE = "ebird_2025_comparison";

  private final String hiveDB;
  private final String sourceTable;
  private final String csvFilePath;

  public static void main(String[] args) {
    EbirdComparisonTool.builder()
        .hiveDB(args[0])
        .sourceTable(args[1])
        .csvFilePath(args.length > 2 ? args[2] : null)
        .build()
        .run();
  }

  @SneakyThrows
  public void run() {
    Objects.requireNonNull(hiveDB, "hiveDB is null");
    Objects.requireNonNull(sourceTable, "sourceTable is null");

    try (SparkSession spark =
        SparkSession.builder()
            .appName("Ebird comparison tool")
            .config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath())
            .enableHiveSupport()
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .getOrCreate()) {
      spark.sql("use " + hiveDB);
      spark.sparkContext().conf().set("hive.exec.compress.output", "false");

      Dataset<Row> rawTable;
      if (csvFilePath != null && !csvFilePath.isEmpty()) {
        rawTable =
            spark
                .read()
                .option("header", "true")
                .option("delimiter", "\t")
                .option("inferSchema", "false")
                .csv(csvFilePath);
      } else {
        rawTable = spark.table(sourceTable);
      }

      Dataset<Row> prodEbird =
          spark
              .table("iceberg.prod_b.occurrence")
              .filter(col("datasetkey").equalTo(EBIRD_DATASET_KEY));
      Dataset<Row> prodEbirdVerbatim =
          prodEbird.select(col("gbifid"), prodEbird.colRegex("`v_.*`"));

      Column rawOccId = col(RAW_TABLE_ALIAS + ".occurrenceid").cast("string");
      Column rawKey =
          when(
                  rawOccId.startsWith(ID_PREFIX),
                  substring(rawOccId, ID_PREFIX.length() + 1, Integer.MAX_VALUE))
              .otherwise(rawOccId);

      Column prodKey = col(PROD_TABLE_ALIAS + ".gbifid");

      Dataset<Row> joined =
          rawTable
              .alias(RAW_TABLE_ALIAS)
              .join(
                  prodEbirdVerbatim.alias(PROD_TABLE_ALIAS), rawKey.equalTo(prodKey), "full_outer");

      List<Column> selectedColumns = new ArrayList<>();
      selectedColumns.add(coalesce(rawKey, prodKey).alias("join_key"));

      for (String columnName : rawTable.columns()) {
        selectedColumns.add(
            col(RAW_TABLE_ALIAS + "." + columnName).alias(RAW_TABLE_ALIAS + "_" + columnName));
      }
      for (String columnName : prodEbirdVerbatim.columns()) {
        selectedColumns.add(
            col(PROD_TABLE_ALIAS + "." + columnName).alias(PROD_TABLE_ALIAS + "_" + columnName));
      }

      selectedColumns.add(
          when(rawOccId.isNotNull().and(prodKey.isNotNull()), lit("MATCH"))
              .when(rawOccId.isNotNull(), lit("ONLY_RAW"))
              .otherwise(lit("ONLY_PROD"))
              .alias("match_status"));

      spark.sql("DROP TABLE IF EXISTS " + COMPARISON_TABLE + " PURGE");

      org.apache.hadoop.fs.Path path =
          new org.apache.hadoop.fs.Path(
              "/stackable/warehouse/" + hiveDB + ".db/" + COMPARISON_TABLE);
      org.apache.hadoop.fs.FileSystem fs =
          path.getFileSystem(spark.sparkContext().hadoopConfiguration());
      if (fs.exists(path)) {
        fs.delete(path, true);
      }

      Dataset<Row> result = joined.select(selectedColumns.toArray(Column[]::new));
      result.write().mode(SaveMode.Overwrite).saveAsTable(COMPARISON_TABLE);
      result.show(false);
    }
  }
}
