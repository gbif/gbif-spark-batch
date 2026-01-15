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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for OccurrenceFeatures.
 *
 * <p>These use a parquet file as input created using the following (CC0 data):
 *
 * <pre>
 *   CREATE TABLE tim.test STORED AS parquet AS
 *   SELECT * FROM prod_h.occurrence
 *   WHERE datasetKey='50c9509d-22c7-4a22-a47d-8c48425ef4a7' AND recordedBy='Tim Robertson'
 * </pre>
 */
public class OccurrenceFeaturesSparkTest extends BaseSparkTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Test to ensure that JSON is generated correctly for nested JSON. */
  @Test
  public void testAsJsonWithMultimedia() throws IOException {
    Dataset<Row> data = sqlContext.read().parquet("src/test/resources/sample.parquet");
    Row first = data.first();

    // read and format the JSON from the first row
    String multimedia = first.getString(first.fieldIndex("ext_multimedia"));
    String formattedMultimedia =
        OBJECT_MAPPER.writeValueAsString(OBJECT_MAPPER.readTree(multimedia));

    RowOccurrenceFeatures features = new RowOccurrenceFeatures(first, null, "ext_multimedia");

    // ensure that the resulting JSON is not escaped
    assertTrue(features.asJson().contains(formattedMultimedia));
  }
}
