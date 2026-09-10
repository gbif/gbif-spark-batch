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

import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** Utility to read the named arguments. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class ArgsParser {

  static Cluster parse(String[] args) {
    if (args.length != 1) {
      throw new IllegalArgumentException("Provided configuration is incorrect");
    }

    String filePath = args[0];
    if (filePath == null || !filePath.endsWith(".properties")) {
      throw new IllegalArgumentException("A properties file is required");
    }
    return parseProperties(filePath);
  }

  @SneakyThrows
  private static Cluster parseProperties(String filepath) {
    File pf = new File(filepath);
    if (!pf.exists()) {
      throw new IllegalArgumentException("Cannot find properties file " + filepath);
    }

    Properties properties = new Properties();
    try (FileReader reader = new FileReader(pf)) {
      properties.load(reader);
    }

    Cluster.ClusterBuilder builder =
        Cluster.builder()
            .hiveWarehousePath(properties.getProperty("hiveWarehousePath"))
            .hiveDB(properties.getProperty("hiveDB"))
            .sourceTable(properties.getProperty("sourceTable"))
            .hiveTablePrefix(properties.getProperty("hiveTablePrefix"))
            .hbaseTable(properties.getProperty("hbaseTable"))
            .hbaseRegions(Integer.parseInt(properties.getProperty("hbaseRegions")))
            .hbaseZK(properties.getProperty("hbaseZK"))
            .targetDir(properties.getProperty("targetDir"))
            .hashCountThreshold(Integer.parseInt(properties.getProperty("hashCountThreshold")));

    log.info("Clustering started with configuration loaded from properties file: {}", builder);
    return builder.build();
  }
}
