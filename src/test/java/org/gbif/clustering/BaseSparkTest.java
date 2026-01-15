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
package org.gbif.clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.junit.jupiter.api.BeforeAll;

/** Base class to keep a shared Spark context for parallel tests. */
public class BaseSparkTest {

  static SparkContext sc;
  static JavaSparkContext jsc;
  static SQLContext sqlContext;

  @BeforeAll
  public static void setup() {
    if (sc == null) {
      SparkConf conf =
          new SparkConf()
              .setMaster("local[*]")
              .setAppName("test")
              .set("spark.driver.bindAddress", "127.0.0.1")
              .set("spark.driver.allowMultipleContexts", "true")
              .set("spark.ui.enabled", "false")
              .set("spark.testing", "true"); // ignore memory check
      sc = SparkContext.getOrCreate(conf);
      jsc = new JavaSparkContext(sc);
      sqlContext = new SQLContext(jsc);
    }
  }
}
