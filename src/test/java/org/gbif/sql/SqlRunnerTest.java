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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class SqlRunnerTest {

  private static final SparkSession SPARK =
      SparkSession.builder().master("local[*]").appName("test").getOrCreate();

  @Test
  public void testDecodeIfBase64() {
    String statement = "CALL iceberg.system.expire_snapshots(table => 'lab.occurrence')";
    String encoded = Base64.getEncoder().encodeToString(statement.getBytes());

    assertEquals(statement, SqlRunner.decodeIfBase64(encoded));
    assertEquals(statement, SqlRunner.decodeIfBase64(statement));
  }

  @Test
  public void testParseScript() {
    List<String> statements =
        SqlRunner.parseScript(
            "-- a leading comment\n"
                + "CREATE TEMP VIEW t1 AS SELECT 1 AS a;\n"
                + "-- another comment\n"
                + "SELECT * FROM t1;\n");

    assertEquals(2, statements.size());
    assertEquals("CREATE TEMP VIEW t1 AS SELECT 1 AS a", statements.get(0));
    assertEquals("SELECT * FROM t1", statements.get(1));
  }

  @Test
  public void testRun() {
    Row first = SPARK.sql("SELECT 1 AS a").first();
    assertEquals(1, first.getInt(0));

    // running through SqlRunner should not throw, and should support both DDL (no results)
    // and queries (with results)
    SqlRunner.run(
        SPARK, List.of("CREATE OR REPLACE TEMP VIEW rt AS SELECT 1 AS a", "SELECT * FROM rt"));
  }
}
