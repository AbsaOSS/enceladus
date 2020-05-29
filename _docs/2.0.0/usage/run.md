---
layout: docs
title: Usage - How to run 
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
redirect_from: /docs/usage/run
---
## Table Of Content

<!-- toc -->
- [Table Of Content](#table-of-content)
- [Requirements and deploy](#requirements-and-deploy)
- [Running Standardization](#running-standardization)
- [Running Conformance](#running-conformance)
- [Helper scripts](#helper-scripts)
<!-- tocstop -->

## Requirements and deploy

For description of requirements and depploy see the [README.md](https://github.com/AbsaOSS/enceladus/blob/master/README.md#how-to-run)
in the the project root.

## Running Standardization
```
<spark home>/spark-submit \
--num-executors <num> \
--executor-memory <num>G \
--master yarn \
--deploy-mode <client/cluster> \
--driver-cores <num> \
--driver-memory <num>G \
--conf "spark.driver.extraJavaOptions=-Dmenas.rest.uri=<menas_api_uri:port> -Dstandardized.hdfs.path=<path_for_standardized_output>-{0}-{1}-{2}-{3} -Dspline.mongodb.url=<mongo_url_for_spline> -Dspline.mongodb.name=<spline_database_name> -Dhdp.version=<hadoop_version>" \
--class za.co.absa.enceladus.standardization.StandardizationJob \
<spark-jobs_<build_version>.jar> \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <date> \
--report-version <data_run_version> \
--raw-format <data_format> \
--row-tag <tag>
```
* Here `row-tag` is a specific option for `raw-format` of type `XML`. For more options for different types please see our WIKI.
* In case Menas is configured for in-memory authentication (e.g. in dev environments), replace `--menas-auth-keytab` with `--menas-credentials-file`

## Running Conformance
```
<spark home>/spark-submit \
--num-executors <num> \
--executor-memory <num>G \
--master yarn \
--deploy-mode <client/cluster> \
--driver-cores <num> \
--driver-memory <num>G \
--conf 'spark.ui.port=29000' \
--conf "spark.driver.extraJavaOptions=-Dmenas.rest.uri=<menas_api_uri:port> -Dstandardized.hdfs.path=<path_of_standardized_input>-{0}-{1}-{2}-{3} -Dconformance.mappingtable.pattern=reportDate={0}-{1}-{2} -Dspline.mongodb.url=<mongo_url_for_spline> -Dspline.mongodb.name=<spline_database_name>" -Dhdp.version=<hadoop_version> \
--packages za.co.absa:enceladus-parent:<version>,za.co.absa:enceladus-conformance:<version> \
--class za.co.absa.enceladus.conformance.DynamicConformanceJob \
<spark-jobs_<build_version>.jar> \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <date> \
--report-version <data_run_version>
```
* In case Menas is configured for in-memory authentication (e.g. in dev environments), replace `--menas-auth-keytab` with `--menas-credentials-file`

## Helper scripts

The Scripts in `scripts` folder can be used to simplify command lines for running Standardization and Conformance jobs.

Steps to configure the scripts are as follows:
* Copy all the scripts in `scripts` directory to a location in your environment.
* Copy `enceladus_env.template.sh` to `enceladus_env.sh`.
* Change `enceladus_env.sh` according to your environment settings.
* Use `run_standardization.sh` and `run_conformance.sh` scripts instead of directly invoking `spark-submit` to run your jobs.

The syntax for running Standardization and Conformance is similar to running them using `spark-submit`. The only difference 
is that you don't have to provide environment-specific settings. The scripts are set to use Spark's _Dynamic Resource Allocation_
(DRA) strategy. The scripts also ensure adaptive execution is enabled together with DRA. Upon shuffle operations, it 
adjusts the data partitioning to target sizes i.g. HDFS blocks sizes. This improves the efficiency of resource usage and 
prevents the issue with small files (when output is split into too many tiny files).

DRA gets auto-disabled, and spark submit falls back to Spark defaults (preserving other custom parameters) when:

- `--num-executors` is set. We assume, that this parameter is set when user knows how many resources are needed exactly. Remove this parameter, to enable DRA back. 
- `--conf-spark-dynamicAllocation-maxExecutors` is set to empty value. The parameter prevents a job from taking over entire cluster. This parameter is set by script defaults to a safe value, but can be overwritten.


The basic command to run **Standardization** becomes:
```
<path to scripts>/run_standardization.sh \
--deploy-mode <client/cluster> \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <date> \
--report-version <data_run_version> \
--raw-format <data_format> \
--row-tag <tag>
```

The basic command to run **Conformance** becomes:
```
<path to scripts>/run_conformance.sh \
--deploy-mode <client/cluster> \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <date> \
--report-version <data_run_version>
```

The list of options for configuring Spark deployment mode in Yarn and resource specification:

|            Option                                                    |   Default   |                         Description                                                                       |
|----------------------------------------------------------------------|-------------|-----------------------------------------------------------------------------------------------------------|
| --deploy-mode **cluster/client**                                     |             | Specifies a Spark Application deployment mode when Spark runs on Yarn. Can be either `client` or `cluster` |
| --set-dra **true/false**                                             | `true`      | Explicitly eneable/disable Dynamic Resource Allocation|
| --conf-spark-dynamicAllocation-minExecutors **n**                    | `0`         | Is strongly recommended to be left to default value of 0 |
| --conf-spark-dynamicAllocation-maxExecutors **n**                    | `4`         | Sets max limit on the number of executors. Should never be empty string or infinity on a shared cluster. Can be adjusted based on expected range on input data |  
| --conf-spark-dynamicAllocation-executorAllocationRatio **float**     | `0.5`       | How many executors per task are allocated. (1/value = tasks per executor) |
| --conf-spark-sql-adaptive-shuffle-targetPostShuffleInputSize **mem** | `134217728` | Target post-shuffle partition size |
| --num-executors **n**                                                |             | Specifies the number of executors to use. |
| --executor-memory **mem**                                            |             | Specifies an amount of memory to request for each executor. See memory specification syntax in Spark. Examples: `4g`, `8g`. |
| --executor-cores **mem**                                             | `1`         | Specifies a number of cores to request for each executor |
| --driver-cores **n**                                                 |             | Specifies a number of CPU cores to allocate for the driver process |
| --driver-memory **mem**                                              |             | Specifies an amount of memory to request for the driver process. See memory specification syntax in Spark. Examples: `4g`, `8g` |
| --persist-storage-level **level**                                    |             | **Advanced** Specifies the storage level to use for persisting intermediate results. Can be one of `NONE`, `DISK_ONLY`, `MEMORY_ONLY`, `MEMORY_ONLY_SER`, `MEMORY_AND_DISK` (default), `MEMORY_AND_DISK_SER`, etc. See more [here](https://spark.apache.org/docs/2.4.4/api/java/index.html?org/apache/spark/storage/StorageLevel.html) |
| --conf-spark-executor-memoryOverhead **mem**                         |             | **Advanced**. The amount of off-heap memory to be allocated per executor, in MiB unless otherwise specified. Sets `spark.executor.memoryOverhead` Spark configuration parameter. See the detailed description [here](http://spark.apache.org/docs/latest/configuration.html#available-properties). See memory specification syntax in Spark. Examples: `4g`, `8g` |
| --conf-spark-memory-fraction **value**                               |             | **Advanced**. Fraction of (heap space - 300MB) used for execution and storage (default=`0.6`). Sets `spark.memory.fraction` Spark configuration parameter. See the detailed description [here](http://spark.apache.org/docs/latest/configuration.html#memory-management) |


For more information on these options see the official documentation on running Spark on Yarn: 
[https://spark.apache.org/docs/latest/running-on-yarn.html](https://spark.apache.org/docs/latest/running-on-yarn.html)

The list of all options for running both Standardization and Conformance:

|            Option                     |                           Description                                                                                                                                                       |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --menas-auth-keytab **filename**      | A keytab file used for Kerberized authentication to Menas. Cannot be used together with `--menas-credentials-file`.                                                                         |
| --menas-credentials-file **filename** | A credentials file containing a login and a password used to authenticate to Menas. Cannot be used together with `--menas-auth-keytab`.                                                     |
| --dataset-name **name**               | A dataset name to be standardized or conformed.                                                                                                                                             |
| --dataset-version **version**         | A version of a dataset to be standardized or conformed.                                                                                                                                     |
| --report-date **YYYY-mm-dd**          | A date specifying a day for which a raw data is landed.                                                                                                                                     |
| --report-version **version**          | A version of the data for a particular day.                                                                                                                                                 |
| --std-hdfs-path **path**              | A path pattern where to put standardized data. The following tokens are expending in the pattern: `{0}` - dataset name, `{1}` - dataset version, `{2}`- report date, `{3}`- report version. |

The list of additional options available for running Standardization:

|            Option                    | Default |                          Description                                                                                                    |
|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| --raw-format **format**              |         | A format for input data. Can be one of `parquet`, `json`, `csv`, `xml`, `cobol`, `fixed-width`                                          |
| --charset **charset**                | `UTF-8` | Specifies a charset to use for `csv`, `json` or `xml`                                                                                   |
| --row-tag **tag**                    |         | A row tag if the input format is `xml`                                                                                                  |
| --header **true/false**              |         | Indicates if in the input CSV data has headers as the first row of each file                                                            |
| --delimiter **character**            | `,`     | Specifies a delimiter character to use for CSV format                                                                                   |
| --csv-quote **character**            | `"`     | Specifies a character to be used as a quote for creating fields that might contain delimiter character                                  |
| --csv-escape **character**           | `\`     | Specifies a character to be used for escaping other characters                                                                          |
| --trimValues **true/false**          |         | Indicates if string fields of fixed with text data should be trimmed                                                                    |
| --is-xcom **true/false**             |         | If `true` a mainframe input file is expected to have XCOM RDW headers                                                                   |
| --folder-prefix **prefix**           |         | Adds a folder prefix before the date tokens                                                                                             |
| --debug-set-raw-path **path**        |         | Override the path of the raw data (used for testing purposes)                                                                           |
| --strict-schema-check **true/false** | `false` | If `true` processing ends the moment a row not adhering to the schema is encountered, `false` proceeds over it with an entry in _errCol | 

The list of additional options available for running Conformance:

|            Option                          |          Default         |                           Description                                        |
|--------------------------------------------|--------------------------|------------------------------------------------------------------------------|
| --mapping-table-pattern **pattern**        | `reportDate={0}-{1}-{2}` | A pattern to look for mapping table for the specified date.<br>The list of possible substitutions: `{0}` - year, `{1}` - month, `{2}` - day of month. Special symbols in the pattern need to be escaped. For example, an empty pattern can be be specified as `\'\'` (single quotes are escaped using a backslash character) |
| --experimental-mapping-rule **true/false** | build-specific and is set in 'application.properties' | If `true`, the experimental optimized mapping rule implementation is used |
| --catalyst-workaround **true/false**       | `true`                   | Turns on (`true`) or off (`false`) workaround for Catalyst optimizer issue. Turn this off only is you encounter timing freeze issues when running Conformance | 
| --autoclean-std-folder **true/false**      |                          | If `true`, the standardized folder will be cleaned automatically after successful execution of a Conformance job |
