# Enceladus TestUtils
___
<!-- toc -->

- [Dataset Comparison](#dataset-comparison)
- [Rest Runner](#rest-runner)
- [E2E Spark Runner](#e2e-spark-runner)
- More to come...

<!-- tocstop -->
## Build
```bash
mvn clean package
```
Or 
```bash
# To be sure there are no problems with older versions
mvn clean package
```

Known to work with: 
- Spark 2.2.2
- Java 1.8.0_191-b12
- Scala 2.12.7
- Hadoop 2.7.5 

## <a name="dataset-comparison" />Dataset Comparison
A Spark job for comparing two data sets. 

### Running
Basic running example
```bash
spark-submit \
--class za.co.absa.enceladus.testutils.datasetComparison.ComparisonJob \
--master local \
--deploy-mode client \
--executor-memory 2g \
--name compare \
--conf "spark.app.id=compare" \
/path/to/jar/file \
--raw-format <format of the reference and new data sets> \
--new-path /path/to/new/data/set \
--ref-path /path/to/referential/data/set \
--out-path /path/to/diff/output
--keys key1,key2,key3
```

#### Where
```bash
Datasets Comparison 
Usage: spark-submit [spark options] TestUtils.jar [options]

  -f, --raw-format <value> format of the raw data (csv, xml, parquet,fixed-width, etc.)
  --row-tag <value>        use the specific row tag instead of 'ROW' for XML format
  --delimiter <value>      use the specific delimiter instead of ',' for CSV format
  --header <value>         use the header option to consider CSV header
  --trim-values <value>    use --trimValues option to trim values in  fixed width file
  --new-path <value>       Path to the new dataset, just generated and to be tested.
  --ref-path <value>       Path to supposedly correct data set.
  --out-path <value>       Path to where the `ComparisonJob` will save the differences. 
                               This will efectivly creat a folder in which you will find two 
                               other folders. expected_minus_actual and actual_minus_expected.
                               Both hold parque data sets of differences. (minus as in is 
                               relative complement
  --keys                   If there are know unique keys, they can be specified for better
                               output. Keys should be specified one by one, with , (comma) 
                               between them.
  --help                   prints this usage text
```

Other configurations are Spark dependant and are out of scope of this README.

##  <a name="rest-runner" />Rest Runner
In progress. Framework for running REST API test.

##  <a name="e2e-spark-runner" />E2E Spark Runner
Runs both Standardization and Confromance on the data provided. After each, a comparison job is run 
to check the results against expected reference data:
- `/ref/tmp/conformance-output/standartized-<DatasetName>-<datasetVersion>-<reportDate>/*.parquet` 
- `/ref/publish/<DatasetName>/enceladus_info_date=<reportDate>/enceladus_info_version=<reportVersion>` 

Basic running example:
```bash
java -cp enceladus-testUtils-0.99.0-SNAPSHOT.jar za.co.absa.enceladus.testutils.E2ESparkRunner \
--menas-credentials-file /path/to/credentials/file \
--dataset-name <datasetName> \
--dataset-version <datasetVersion> \
--report-date <reportData> \
--report-version <reportVersion> \
--raw-format <rawFormat> 
--keys <key1,key2,...> 
--spark-conf-file /path/to/spark/configuration/file
```
