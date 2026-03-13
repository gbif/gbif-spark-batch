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

import static org.gbif.clustering.HashUtilities.recordHashes;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.clustering.parsers.OccurrenceRelationships;
import org.gbif.clustering.parsers.RelationshipAssertion;
import scala.Tuple2;

/**
 * Regenerates the HBase and Hive tables for a data clustering run. This will read the occurrence
 * Hive table, generate HFiles for relationships and create intermediate tables in Hive for
 * diagnostics, replacing any that exist. Finally, the HBase table is truncated preserving its
 * partitioning and bulk loaded with new data. <br>
 * This process works on the deliberate design principle that readers of the table will have
 * implemented a retry mechanism for the brief outage during the table swap which is expected to be
 * on an e.g. weekly basis.
 */
@Builder
@Data
@Slf4j
public class Cluster2 implements Serializable {
  private String hiveWarehousePath; // e.g. /stackable/warehouse/prod.db
  private String hiveDB;
  private String sourceTable;
  private String hiveTablePrefix;
  private String hbaseTable;
  private int hbaseRegions;
  private String hbaseZK;
  private String targetDir;
  private int hashCountThreshold;

  private static final String TAXONOMY_KEY =
      "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c"; // GBIF Backbone
  private static final StructType HASH_ROW_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("gbifId", DataTypes.StringType, false),
            DataTypes.createStructField("datasetKey", DataTypes.StringType, false),
            DataTypes.createStructField("hash", DataTypes.StringType, false)
          });

  private static final StructType RELATIONSHIP_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id1", DataTypes.StringType, false),
            DataTypes.createStructField("id2", DataTypes.StringType, false),
            DataTypes.createStructField("reasons", DataTypes.StringType, false),
            DataTypes.createStructField("dataset1", DataTypes.StringType, false),
            DataTypes.createStructField("dataset2", DataTypes.StringType, false),
            DataTypes.createStructField("o1", DataTypes.StringType, false),
            DataTypes.createStructField("o2", DataTypes.StringType, false)
          });

  public static void main(String[] args) throws IOException, AnalysisException {
    ArgsParser2.parse(args).run();
  }

  /** Run the full process, generating relationships and refreshing the HBase table. */
  private void run() throws IOException {
    try (FileSystem fileSystem = FileSystem.get(hadoopConf());
        SparkSession spark =
            SparkSession.builder()
                .appName("Occurrence clustering")
                .config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath())
                .enableHiveSupport()
                .config("spark.sql.catalog.iceberg.type", "hive")
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                .getOrCreate()) {
      spark.sql("use " + hiveDB);

      // remove target HFile directory and temp tables
      removeTargetDir(fileSystem);
      dropPreviousTables(fileSystem, spark);

      // execute
      createInputTable(spark);
      createCandidatePairs(spark);
      generateRelationships(spark);
      generateHFiles(spark);
      replaceHBaseTable();

      removeTargetDir(fileSystem); // clean up working directory
    }
  }

  private String sourceTableQualifiedName() {
    return "iceberg." + hiveDB + "." + sourceTable;
  }

  /** Reads the input and creates a smaller table with only the fields needed for clustering. */
  private void createInputTable(SparkSession spark) {
    // All taxa keys are converted to String to allow shared routines between GBIF and ALA
    // (https://github.com/gbif/pipelines/issues/484)
    spark
        .sql(
            String.format(
                "SELECT"
                    + " gbifId, datasetKey, basisOfRecord, publishingorgkey AS publishingOrgKey, datasetName, publisher AS publishingOrgName, "
                    + "  CAST(kingdomKey AS String) AS kingdomKey, CAST(phylumKey AS String) AS phylumKey, CAST(classKey AS String) AS classKey, "
                    + "  CAST(orderKey AS String) AS orderKey, CAST(familyKey AS String) AS familyKey, CAST(genusKey AS String) AS genusKey, "
                    + "  CAST(speciesKey AS String) AS speciesKey, CAST(acceptedTaxonKey AS String) AS acceptedTaxonKey, CAST(taxonKey AS String) AS taxonKey, "
                    + "  scientificName, acceptedScientificName, kingdom, phylum, order, family, genus, species, genericName, specificEpithet, taxonRank, "
                    + "  typeStatus, preparations, "
                    + "  decimalLatitude, decimalLongitude, countryCode, "
                    + "  year, month, day, from_unixtime(eventDateGte) AS eventDate, "
                    + "  recordNumber, fieldNumber, occurrenceID, otherCatalogNumbers, institutionCode, collectionCode, catalogNumber, "
                    + "  recordedBy, recordedByID, "
                    + "  ext_multimedia AS media "
                    + "FROM %s "
                    + "WHERE "
                    + "  speciesKey IS NOT NULL AND "
                    + "  NOT contains(taxonomicissue['%s'], 'TAXON_MATCH_HIGHERRANK') ",
                sourceTableQualifiedName(), TAXONOMY_KEY))
        .write()
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable(hiveTablePrefix + "_input");
  }

  /**
   * Reads the input, and through a series of record hashing creates a table of candidate record
   * pairs for proper comparison.
   */
  private void createCandidatePairs(SparkSession spark) {
    // Generate the hashes
    FlatMapFunction<Row, Row> toHashes = row -> recordHashes(new RowOccurrenceFeatures(row));
    spark
        .sql(
            String.format(
                "SELECT"
                    + "  gbifId, datasetKey, basisOfRecord, typeStatus, "
                    + "  taxonKey, speciesKey, "
                    + "  decimalLatitude, decimalLongitude, year, month, day, recordedBy, "
                    + "  recordNumber, fieldNumber, occurrenceID, otherCatalogNumbers, institutionCode, "
                    + "  collectionCode, catalogNumber "
                    + "FROM %s",
                hiveTablePrefix + "_input"))
        .flatMap(toHashes, Encoders.row(HASH_ROW_SCHEMA))
        .write()
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable(hiveTablePrefix + "_hashes");

    // To avoid NxN, filter to a threshold to exclude e.g. gut worm analysis datasets
    spark
        .sql(
            String.format(
                "SELECT gbifId, datasetKey, hash "
                    + "FROM ( "
                    + "  SELECT gbifId, datasetKey, hash, COUNT(*) OVER (PARTITION BY hash) AS cnt "
                    + "  FROM %s_hashes "
                    + ") sub "
                    + "WHERE cnt <= %d AND cnt > 1",
                hiveTablePrefix, hashCountThreshold))
        .write()
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable(hiveTablePrefix + "_hashes_filtered");

    // distinct cross join to generate the candidate pairs for comparison
    spark
        .sql(
            String.format(
                "SELECT "
                    + "t1.gbifId as id1, t1.datasetKey as ds1, t2.gbifId as id2, t2.datasetKey as ds2 "
                    + "FROM %1$s_hashes_filtered t1 JOIN %1$s_hashes_filtered t2 ON t1.hash = t2.hash "
                    + "WHERE "
                    + "  t1.gbifId < t2.gbifId AND "
                    + "  t1.datasetKey != t2.datasetKey "
                    + "GROUP BY t1.gbifId, t1.datasetKey, t2.gbifId, t2.datasetKey",
                hiveTablePrefix))
        .write()
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable(hiveTablePrefix + "_candidates");
  }

  /** Reads the candidate pairs table, and runs the record to record comparison. */
  private void generateRelationships(SparkSession spark) {
    // Spark DF naming convention requires that we alias each term to avoid naming collision while
    // still having named fields to access (i.e. not relying on the column number of the term).
    List<String> columns =
        Arrays.asList(
            "gbifId",
            "datasetKey",
            "basisOfRecord",
            "publishingOrgKey",
            "datasetName",
            "publishingOrgName",
            "kingdomKey",
            "phylumKey",
            "classKey",
            "orderKey",
            "familyKey",
            "genusKey",
            "speciesKey",
            "acceptedTaxonKey",
            "taxonKey",
            "scientificName",
            "acceptedScientificName",
            "kingdom",
            "phylum",
            "order",
            "family",
            "genus",
            "species",
            "genericName",
            "specificEpithet",
            "taxonRank",
            "typeStatus",
            "preparations",
            "decimalLatitude",
            "decimalLongitude",
            "countryCode",
            "year",
            "month",
            "day",
            "eventDate",
            "recordNumber",
            "fieldNumber",
            "occurrenceID",
            "otherCatalogNumbers",
            "institutionCode",
            "collectionCode",
            "catalogNumber",
            "recordedBy",
            "recordedByID",
            "media");

    // creates e.g. t1.datasetKey AS t1_datasetKey, t2.datasetKey AS t2_datasetKey
    String t1Columns =
        columns.stream()
            .map(col -> "t1." + col + " AS t1_" + col)
            .collect(Collectors.joining(", "));
    String t2Columns =
        columns.stream()
            .map(col -> "t2." + col + " AS t2_" + col)
            .collect(Collectors.joining(", "));

    // expand the candidates with the fields needed to compare
    spark
        .sql(
            String.format(
                "SELECT %1$s, %2$s "
                    + "FROM %3$s h "
                    + "JOIN %4$s t1 ON h.id1 = t1.gbifId "
                    + "JOIN %4$s t2 ON h.id2 = t2.gbifId",
                t1Columns, t2Columns, hiveTablePrefix + "_candidates", hiveTablePrefix + "_input"))
        .write()
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable(hiveTablePrefix + "_candidates_processed");

    // Compare all candidate pairs and generate the relationships
    spark
        .sql(String.format("SELECT * FROM %s", hiveTablePrefix + "_candidates_processed"))
        .flatMap((FlatMapFunction<Row, Row>) this::relateRecords, Encoders.row(RELATIONSHIP_SCHEMA))
        .write()
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .saveAsTable(hiveTablePrefix + "_relationships");
  }

  /** Runs the record to record comparison */
  private Iterator<Row> relateRecords(Row row) throws IOException {
    Set<Row> relationships = new HashSet<>();

    RowOccurrenceFeatures o1 = new RowOccurrenceFeatures(row, "t1_", "t1_media");
    RowOccurrenceFeatures o2 = new RowOccurrenceFeatures(row, "t2_", "t2_media");
    RelationshipAssertion<RowOccurrenceFeatures> assertions =
        OccurrenceRelationships.generate(o1, o2);

    // store any relationship bidirectionally matching RELATIONSHIP_SCHEMA
    if (assertions != null) {
      relationships.add(
          RowFactory.create(
              assertions.getOcc1().get("gbifId"),
              assertions.getOcc2().get("gbifId"),
              assertions.getJustificationAsDelimited(),
              assertions.getOcc1().get("datasetKey"),
              assertions.getOcc2().get("datasetKey"),
              assertions.getOcc1().asJson(),
              assertions.getOcc2().asJson()));

      relationships.add(
          RowFactory.create(
              assertions.getOcc2().get("gbifId"),
              assertions.getOcc1().get("gbifId"),
              assertions.getJustificationAsDelimited(),
              assertions.getOcc2().get("datasetKey"),
              assertions.getOcc1().get("datasetKey"),
              assertions.getOcc2().asJson(),
              assertions.getOcc1().asJson()));
    }
    return relationships.iterator();
  }

  /** Partitions the relationships to match the target table layout and creates the HFiles. */
  private void generateHFiles(SparkSession spark) throws IOException {
    // convert to HFiles, prepared with modulo salted keys
    JavaPairRDD<Tuple2<String, String>, String> sortedRelationships =
        spark
            .sql(String.format("SELECT * FROM %s", hiveTablePrefix + "_relationships"))
            .javaRDD()
            .flatMapToPair(
                row -> {
                  String id1 = row.getString(0);
                  String id2 = row.getString(1);

                  // salt only on the id1 to enable prefix scanning using an occurrence ID
                  int salt = Math.abs(id1.hashCode()) % hbaseRegions;
                  String saltedRowKey = salt + ":" + id1 + ":" + id2;

                  List<Tuple2<Tuple2<String, String>, String>> cells = new ArrayList<>();
                  cells.add(new Tuple2<>(new Tuple2<>(saltedRowKey, "id1"), id1));
                  cells.add(new Tuple2<>(new Tuple2<>(saltedRowKey, "id2"), id2));
                  cells.add(new Tuple2<>(new Tuple2<>(saltedRowKey, "reasons"), row.getString(2)));
                  cells.add(new Tuple2<>(new Tuple2<>(saltedRowKey, "dataset1"), row.getString(3)));
                  cells.add(new Tuple2<>(new Tuple2<>(saltedRowKey, "dataset2"), row.getString(4)));
                  cells.add(
                      new Tuple2<>(new Tuple2<>(saltedRowKey, "occurrence1"), row.getString(5)));
                  cells.add(
                      new Tuple2<>(new Tuple2<>(saltedRowKey, "occurrence2"), row.getString(6)));

                  return cells.iterator();
                })
            .repartitionAndSortWithinPartitions(
                new SaltPrefixPartitioner(hbaseRegions), new Tuple2StringComparator());

    sortedRelationships
        .mapToPair(
            cell -> {
              ImmutableBytesWritable k = new ImmutableBytesWritable(Bytes.toBytes(cell._1._1));
              Cell row =
                  new KeyValue(
                      Bytes.toBytes(cell._1._1), // key
                      Bytes.toBytes("o"), // column family
                      Bytes.toBytes(cell._1._2), // cell
                      Bytes.toBytes(cell._2) // cell value
                      );
              return new Tuple2<>(k, row);
            })
        .saveAsNewAPIHadoopFile(
            targetDir,
            ImmutableBytesWritable.class,
            KeyValue.class,
            HFileOutputFormat2.class,
            hadoopConf());
  }

  /** Truncates the target table preserving its layout and loads in the HFiles. */
  private void replaceHBaseTable() throws IOException {
    Configuration conf = hadoopConf();
    try (Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(hbaseTable));
        Admin admin = connection.getAdmin()) {

      BulkLoadHFilesTool loader = new BulkLoadHFilesTool(conf);

      // bulkload requires files to be in hbase ownership
      FsShell shell = new FsShell(conf);
      try {
        log.info("Executing chown -R hbase:hbase for {}", targetDir);
        shell.run(new String[] {"-chown", "-R", "hbase:hbase", targetDir});
      } catch (Exception e) {
        throw new IOException("Unable to modify FS ownership to hbase", e);
      }

      log.info("Truncating table {}", hbaseTable);
      Instant start = Instant.now();
      admin.disableTable(table.getName());
      admin.truncateTable(table.getName(), true);
      log.info(
          "Table {} truncated in {}",
          hbaseTable,
          Duration.between(start, Instant.now()).toMillis());
      log.info("Loading table {} from {}", hbaseTable, targetDir);
      loader.bulkLoad(table.getName(), new Path(targetDir));

      log.info(
          "Table {} truncated and reloaded in {}",
          hbaseTable,
          Duration.between(start, Instant.now()).toMillis());

      admin.majorCompact(table.getName()); // bring data locality

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /** Removes the target directory provided it is in the /tmp location */
  private void removeTargetDir(FileSystem fileSystem) throws IOException {
    // defensive, cleaning only /tmp in hdfs (we assume people won't do /tmp/../...)
    String regex = "/tmp/.+";
    if (targetDir.matches(regex)) {
      Path p = new Path(targetDir);
      assert !p.toString().contains(".."); // defensive
      log.info(
          "Deleting working directory {} which translates to [-rm -r -skipTrash {} ]",
          targetDir,
          targetDir);
      fileSystem.delete(p, true);
    } else {
      throw new IllegalArgumentIOException("Target directory must be within /tmp");
    }
  }

  /** Drops all temporary tables and the underlying directories. */
  private void dropPreviousTables(FileSystem fileSystem, SparkSession spark) throws IOException {
    dropTable(spark, fileSystem, hiveTablePrefix + "_input");
    dropTable(spark, fileSystem, hiveTablePrefix + "_hashes");
    dropTable(spark, fileSystem, hiveTablePrefix + "_hash_counts");
    dropTable(spark, fileSystem, hiveTablePrefix + "_hashes_filtered");
    dropTable(spark, fileSystem, hiveTablePrefix + "_candidates");
    dropTable(spark, fileSystem, hiveTablePrefix + "_candidates_processed");
    dropTable(spark, fileSystem, hiveTablePrefix + "_relationships");
  }

  /** Utility to cleanly drop the table and target directory */
  private void dropTable(SparkSession spark, FileSystem fs, String table) throws IOException {
    spark.sql("DROP TABLE IF EXISTS " + table + " PURGE");

    // Remove the directory as well (https://github.com/gbif/clustering/issues/4)
    assert hiveWarehousePath.length() >= 4; // /dev, /test, /stackable/warehouse/prod.db
    log.info("Deleting " + hiveWarehousePath + "/" + table);
    Path p = new Path(hiveWarehousePath, table);
    assert !p.toString().contains(".."); // defensive
    fs.delete(p, true);
  }

  /** Creates the Hadoop configuration suitable for HDFS and HBase use. */
  private Configuration hadoopConf() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", hbaseZK);
    conf.set(FileOutputFormat.COMPRESS, "true");
    conf.setClass(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class, CompressionCodec.class);

    try (Connection c = ConnectionFactory.createConnection(conf)) {
      Job job = Job.getInstance(conf, "Clustering"); // name not actually used
      Table table = c.getTable(TableName.valueOf(hbaseTable));
      HFileOutputFormat2.configureIncrementalLoad(job, table, table.getRegionLocator());
      return job.getConfiguration(); // job created a copy of the conf
    }
  }

  /** Necessary as the Tuple2 does not implement a comparator in Java */
  static class Tuple2StringComparator implements Comparator<Tuple2<String, String>>, Serializable {
    @Override
    public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
      return o1._1.equals(o2._1) ? o1._2.compareTo(o2._2) : o1._1.compareTo(o2._1);
    }
  }
}
