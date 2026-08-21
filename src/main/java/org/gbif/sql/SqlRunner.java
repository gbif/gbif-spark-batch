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
package org.gbif.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Executes arbitrary Spark SQL statements, with no external file required. Statements are supplied
 * either as individual command line arguments (one statement per argument) or, if no arguments are
 * given, as a single script read from standard input, where statements are separated by semicolons
 * and lines starting with {@code --} are treated as comments. Intended for ad-hoc administrative
 * tasks (e.g. DDL) against the Hive/Iceberg catalogs used by the other jobs in this project.
 */
@Slf4j
public class SqlRunner {

  public static void main(String[] args) throws IOException {
    List<String> statements = args.length > 0 ? Arrays.asList(args) : readStatementsFromStdin();

    try (SparkSession spark = buildSparkSession()) {
      run(spark, statements);
    }
  }

  static SparkSession buildSparkSession() {
    return SparkSession.builder()
        .appName("Spark SQL runner")
        .config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath())
        .enableHiveSupport()
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        // required for Iceberg's CALL procedure syntax (expire_snapshots, etc.), which is added
        // by this extension rather than being part of Spark's own SQL grammar
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate();
  }

  /** Executes the given statements in order, logging progress and showing any results. */
  static void run(SparkSession spark, List<String> statements) {
    for (String statement : statements) {
      log.info("Executing: {}", statement);
      Dataset<Row> result = spark.sql(statement);
      if (result.schema().fields().length > 0) {
        result.show(1000, false);
      }
    }
  }

  /** Reads a full SQL script from standard input, splitting it into individual statements. */
  static List<String> readStatementsFromStdin() throws IOException {
    StringBuilder content = new StringBuilder();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        content.append(line).append('\n');
      }
    }
    return parseScript(content.toString());
  }

  /**
   * Splits a script into individual, non-empty SQL statements, ignoring full-line {@code --}
   * comments.
   */
  static List<String> parseScript(String content) {
    // strip full-line comments before splitting, so a ';' in a comment can't create a bogus
    // empty statement
    String uncommented =
        Arrays.stream(content.split("\n"))
            .filter(line -> !line.trim().startsWith("--"))
            .collect(Collectors.joining("\n"));

    List<String> statements = new ArrayList<>();
    for (String statement : uncommented.split(";")) {
      String trimmed = statement.trim();
      if (!trimmed.isEmpty()) {
        statements.add(trimmed);
      }
    }
    return statements;
  }
}
