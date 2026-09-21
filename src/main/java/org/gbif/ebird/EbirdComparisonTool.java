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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import lombok.Builder;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.when;

@Builder(toBuilder = true)
public class EbirdComparisonTool implements Serializable {

  private static final String ID_PREFIX = "URN:catalog:CLO:EBIRD:";
  private static final String RAW_TABLE_ALIAS = "raw_table";
  private static final String PROD_TABLE_ALIAS = "prod_table";

  private final String hiveDB;
  private final String sourceTable;

  public static void main(String[] args) {
    EbirdComparisonTool.builder().hiveDB(args[0]).sourceTable(args[1]).build().run();
  }

  public void run() {
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

      Dataset<Row> rawTable = spark.table("hive." + hiveDB + "." + sourceTable);
      Dataset<Row> prodTable = spark.table("iceberg.prod_b.occurrence");

      String prodJoinColumn = "gbifid";

      // join column in raw table
      Column baseColumn = col(RAW_TABLE_ALIAS + ".occurrenceid").cast("string");
      Column rawJoinColumn =
        when(
          baseColumn.startsWith(ID_PREFIX),
          substring(baseColumn, ID_PREFIX.length() + 1, Integer.MAX_VALUE))
          .otherwise(baseColumn);

      Dataset<Row> joined =
        rawTable
          .alias(RAW_TABLE_ALIAS)
          .join(
            prodTable.alias(PROD_TABLE_ALIAS),
            rawJoinColumn.equalTo(prodJoinColumn),
            "full_outer");

      List<Column> selectedColumns = new ArrayList<>();
      selectedColumns.add(
        coalesce(
          col(RAW_TABLE_ALIAS + "." + rawJoinColumn),
          col(PROD_TABLE_ALIAS + "." + prodJoinColumn))
          .alias("join_key"));

      for (String columnName : rawTable.columns()) {
        selectedColumns.add(
          col(RAW_TABLE_ALIAS + "." + columnName).alias(RAW_TABLE_ALIAS + "_" + columnName));
      }

      for (String columnName : prodTable.columns()) {
        selectedColumns.add(
          col(PROD_TABLE_ALIAS + "." + columnName).alias(PROD_TABLE_ALIAS + "_" + columnName));
      }

      selectedColumns.add(
        when(
          col(RAW_TABLE_ALIAS + "." + rawJoinColumn)
            .isNotNull()
            .and(col(PROD_TABLE_ALIAS + "." + prodJoinColumn).isNotNull()),
          lit("MATCH"))
          .when(col(RAW_TABLE_ALIAS + "." + rawJoinColumn).isNotNull(), lit("ONLY_RAW"))
          .otherwise(lit("ONLY_PROD"))
          .alias("match_status"));

      Dataset<Row> result = joined.select(selectedColumns.toArray(Column[]::new));
      result.write().mode(SaveMode.Overwrite).saveAsTable("ebird_2025_comparison");
      result.show(false);
    }
  }
}
