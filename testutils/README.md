# Enceladus TestUtils
___
<!-- toc -->

- [Dataset Comparison](#dataset-comparison)
- [Rest Runner](#rest-runner)
- More to come...

<!-- tocstop -->

## <a name="dataset-comparison" />Dataset Comparison
A Spark job for comparing two data sets. 

### Build
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

### Running
```
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
```

#### Where
```
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
  --help                   prints this usage text

```

Other configurations are Spark dependant and are out of scope of this README.

##  <a name="rest-runner" />Rest Runner
In progress. Framework for running REST API test.