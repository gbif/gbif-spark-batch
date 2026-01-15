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
package org.gbif.sequence;

import java.io.File;
import java.io.Serializable;
import lombok.Builder;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

@Builder(toBuilder = true)
public class FastaBuilder implements Serializable {
  private final String hiveDB;
  private final String targetFile;

  public static void main(String[] args) {
    FastaBuilder.builder().hiveDB(args[0]).targetFile(args[1]).build().run();
  }

  public void run() {
    try (SparkSession spark =
        SparkSession.builder()
            .appName("FASTA Builder")
            .config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath())
            .enableHiveSupport()
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .getOrCreate()) {
      String fs = spark.sparkContext().hadoopConfiguration().get("fs.defaultFS");
      System.out.println("fs.defaultFS=" + fs);
      spark.sql("use " + hiveDB);
      spark.sparkContext().conf().set("hive.exec.compress.output", "false");

      UDF1<String, String> normalize =
          s ->
              s != null && !s.isEmpty()
                  ? s.toUpperCase().replaceAll("[^ACGTURYSWKMBDHVN]", "")
                  : null;

      spark.udf().register("normalize", normalize, DataTypes.StringType);

      String sql =
          String.format(
              "      WITH sequences AS ("
                  + "  SELECT normalize(dnasequence) AS seq "
                  + "  FROM iceberg.%s.occurrence_ext_gbif_dnaderiveddata "
                  + "  WHERE dnasequence IS NOT NULL AND length(dnasequence) > 0 "
                  + "  GROUP BY normalize(dnasequence)"
                  + ") "
                  + "SELECT concat('>', md5(seq), '\n', seq) AS f FROM sequences",
              hiveDB);

      spark.sql(sql).write().mode(SaveMode.Overwrite).text(targetFile);
    }
  }
}
