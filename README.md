## GBIF Spark Batch

This project contains the simple batch jobs that use Apache Spark.
Common across all jobs is that they bring in little or no significant dependencies beyond core infrastructure components.

- [clustering](./README-clustering.md) generates the HBase table of relationships between occurrence records
- [fasta](./README-fasta.md) exports the distinct, cleaned sequences from occurrence records in a FASTA file
- [dataset export tool](./README-dataset-export-comparison-tool.md) compares the export of a dataset with the currently ingested version in production.
- [dataset reducer tool](./README-dataset-export-comparison-tool.md) reduces the size of a sampling event dataset with the humboldt and occurrence extension.
