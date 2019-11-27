# Enceladus

### <a name="build_status"/>Build Status
| master | develop |
| ------------- | ------------- |
| [![Build Status](https://opensource.bigusdatus.com/jenkins/buildStatus/icon?job=Absa-OSS-Projects%2Fenceladus%2Fmaster)](https://opensource.bigusdatus.com/jenkins/job/Absa-OSS-Projects/job/enceladus/job/master/) | [![Build Status](https://opensource.bigusdatus.com/jenkins/buildStatus/icon?job=Absa-OSS-Projects%2Fenceladus%2Fdevelop)](https://opensource.bigusdatus.com/jenkins/job/Absa-OSS-Projects/job/enceladus/job/develop/) |
### <a name="code_quality_status"/>Code Quality Status
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=AbsaOSS_enceladus&metric=alert_status)](https://sonarcloud.io/dashboard?id=AbsaOSS_enceladus)

___

<!-- toc -->
- [What is Enceladus?](#what-is-enceladus)
    - [Menas](#menas)
    - [Standardization](#standardization)
    - [Conformance](#conformance)
- [How to build](#how-to-build)
- [How to run](#how-to-run)
- [How to contribute](#how-to-contribute)
- [Documentation](#documentation)
<!-- tocstop -->

## What is Enceladus?
**Enceladus** is a **Dynamic Conformance Engine** which allows data from different formats to be standardized to parquet and conformed to group-accepted common reference (e.g. data for country designation which are **DE** in one source system and **Deutschland** in another, can be conformed to **Germany**).

The project is comprised of three main components:
### Menas
This is the user-facing web client, used to **specify the standardization schema**, and **define the steps required to conform** a dataset.  
There are three models used to do this:
 - **Dataset**: Specifies where the dataset will be read from on HDFS (**RAW**), the conformance rules that will be applied to it, and where it will land on HDFS once it is conformed (**PUBLISH**)
 - **Schema**: Specifies the schema towards which the dataset will be standardized
 - **Mapping Table**: Specifies where tables with master reference data can be found (parquet on HDFS), which are used when applying Mapping conformance rules (e.g. the dataset uses **Germany**, which maps to the master reference **DE** in the mapping table)

### Standardization
This is a Spark job which reads an input dataset in any of the supported formats and **produces a parquet dataset with the Menas-specified schema** as output. 

### Conformance
This is a Spark job which **applies the Menas-specified conformance rules to the standardized dataset**.

## How to build
#### Build requirements:
 - **Maven 3.5.4+**
 - **Java 8**

Each module provides configuration file templates with reasonable default values.
Make a copy of the `*.properties.template` and `*.conf.template` files in each module's `src/resources` directory removing the `.template` extension. 
Ensure the properties there fit your environment.

#### Build commands:

- Without tests: `mvn clean package -DskipTests `
- With unit tests: `mvn clean package`
- With integration tests: `mvn clean package -Pintegration`
- With component preload file generated: `mvn clean package -PgenerateComponentPreload`

#### Test coverage:
- Test coverage: `mvn scoverage:report`

The coverage reports are written in each module's `target` directory and aggregated in the root `target` directory.

## How to run
#### Menas requirements:
- [**Tomcat 8.5/9.0** installation](https://tomcat.apache.org/download-90.cgi)
- [**MongoDB 4.0** installation](https://docs.mongodb.com/manual/administration/install-community/)
- [**Spline UI deployment**](https://absaoss.github.io/spline/) - place the [spline.war](https://search.maven.org/remotecontent?filepath=za/co/absa/spline/spline-web/0.3.9/spline-web-0.3.9.war)
 in your Tomcat webapps directory (rename after downloading to _spline.war_); NB! don't forget to set up the `spline.mongodb.url` configuration for the _war_
- **HADOOP_CONF_DIR** environment variable, pointing to the location of your hadoop configuration (pointing to a hadoop installation)

The _Spline UI_ can be omitted; in such case the **Menas** `za.co.absa.enceladus.spline.urlTemplate` setting should be set to empty string. 

#### Deploying Menas
Simply copy the **menas.war** file produced when building the project into Tomcat's webapps directory. 

#### Speed up initial loading time of menas
- Build the project with the generateComponentPreload profile. Component preload will greatly reduce the number of HTTP requests required for the initial load of Menas
- Enable the HTTP compression
- Configure `spring.resources.cache.cachecontrol.max-age` in `application.properties` of Menas for caching of static resources

#### Standardization and Conformance requirements:
- [**Spark 2.4.4 (Scala 2.11)** installation](https://spark.apache.org/downloads.html)
- [**Hadoop 2.7** installation](https://hadoop.apache.org/releases.html)
- **Menas** running instance
- **Menas Credentials File** in your home directory or on HDFS (a configuration file for authenticating the Spark jobs with Menas) 
   - **Use with in-memory authentication**
e.g. `~/menas-credential.properties`:
```
username=user
password=changeme
```
- **Menas Keytab File** in your home directory or on HDFS
   - **Use with kerberos authentication**, see [link](https://kb.iu.edu/d/aumh) for details on creating keytab files
 - **Directory structure** for the **RAW** dataset should follow the convention of `<path_to_dataset_in_menas>/<year>/<month>/<day>/v<dataset_version>`. This date is specified with the `--report-date` option when running the **Standardization** and **Conformance** jobs.
 - **_INFO file** must be present along with the **RAW** data on HDFS as per the above directory structure. This is a file tracking control measures via [Atum](https://github.com/AbsaOSS/atum), an example can be found [here](examples/data/input/_INFO).

#### Running Standardization
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
<standardization_<build_version>.jar> \
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

#### Running Conformance
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
<conformance_<build_version>.jar> \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <date> \
--report-version <data_run_version>
```
* In case Menas is configured for in-memory authentication (e.g. in dev environments), replace `--menas-auth-keytab` with `--menas-credentials-file`

#### Helper scripts for running Standardization and Conformance

The Scripts in `scripts` folder can be used to simplify command lines for running Standardization and Conformance jobs.

Steps to configure the scripts are as follows:
* Copy all the scripts in `scripts` directory to a location in your environment.
* Copy `enceladus_env.template.sh` to `enceladus_env.sh`.
* Change `enceladus_env.sh` according to your environment settings.
* Use `run_standardization.sh` and `run_conformance.sh` scripts instead of directly invoking `spark-submit` to run your jobs.

The syntax for running Standardization and Conformance is similar to running them using `spark-submit`. The only difference is that
you don't have to provide environment-specific settings. Several resource options, like driver memory and driver cores also have
default values and can be omitted. The number of executors is still a mandatory parameter.

The basic command to run Standardization becomes:
```
<path to scripts>/run_standardization.sh \
--num-executors <num> \
--deploy-mode <client/cluster> \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <date> \
--report-version <data_run_version> \
--raw-format <data_format> \
--row-tag <tag>
```

The basic command to run Conformance becomes:
```
<path to scripts>/run_conformance.sh \
--num-executors <num> \
--deploy-mode <client/cluster> \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <date> \
--report-version <data_run_version>
```

The list of options for configuring Spark deployment mode in Yarn and resource specification:

|            Option                            |                           Description                                                                       |
|----------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| --deploy-mode **cluster/client**             | Specifies a Spark Application deployment mode when Spark runs on Yarn. Can be either `client` or `cluster`. |
| --num-executors **n**                        | Specifies the number of executors to use. |
| --executor-memory **mem**                    | Specifies an amount of memory to request for each executor. See memory specification syntax in Spark. Examples: `4g`, `8g`. |
| --executor-cores **mem**                     | Specifies a number of cores to request for each executor (default=1). |
| --driver-cores **n**                         | Specifies a number of CPU cores to allocate for the driver process. |
| --driver-memory **mem**                      | Specifies an amount of memory to request for the driver process. See memory specification syntax in Spark. Examples: `4g`, `8g`. |
| --persist-storage-level **level**            | **Advanced** Specifies the storage level to use for persisting intermediate results. Can be one of `NONE`, `DISK_ONLY`, `MEMORY_ONLY`, `MEMORY_ONLY_SER`, `MEMORY_AND_DISK` (default), `MEMORY_AND_DISK_SER`, etc. See more [here](https://spark.apache.org/docs/2.4.4/api/java/index.html?org/apache/spark/storage/StorageLevel.html). |
| --conf-spark-executor-memoryOverhead **mem** | **Advanced**. The amount of off-heap memory to be allocated per executor, in MiB unless otherwise specified. Sets `spark.executor.memoryOverhead` Spark configuration parameter. See the detailed description [here](http://spark.apache.org/docs/latest/configuration.html#available-properties). See memory specification syntax in Spark. Examples: `4g`, `8g`. |
| --conf-spark-memory-fraction **value**       | **Advanced**. Fraction of (heap space - 300MB) used for execution and storage (default=`0.6`). Sets `spark.memory.fraction` Spark configuration parameter. See the detailed description [here](http://spark.apache.org/docs/latest/configuration.html#memory-management). |


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

|            Option             |                           Description                                                                                           |
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| --raw-format **format**       | A format for input data. Can be one of `parquet`, `json`, `csv`, `xml`, `cobol`, `fixed-width`.                                 |
| --charset **charset**         | Specifies a charset to use for `csv`, `json` or `xml`. Default is `UTF-8`.                                                      |
| --row-tag **tag**             | A row tag if the input format is `xml`.                                                                                         |
| --header **true/false**       | Indicates if in the input CSV data has headers as the first row of each file.                                                   |
| --delimiter **character**     | Specifies a delimiter character to use for CSV format. By default `,` is used.                                                  |
| --csv-quote **character**     | Specifies a character to be used as a quote for creating fields that might contain delimiter character. By default `"` is used. |
| --csv-escape **character**    | Specifies a character to be used for escaping other characters. By default '&#92;' (backslash) is used.                         |
| --trimValues **true/false**   | Indicates if string fields of fixed with text data should be trimmed.                                                           |
| --is-xcom **true/false**      | If `true` a mainframe input file is expected to have XCOM RDW headers.                                                          |
| --folder-prefix **prefix**    | Adds a folder prefix before the date tokens.                                                                                    |
| --debug-set-raw-path **path** | Override the path of the raw data (used for testing purposes).                                                                  |

The list of additional options available for running Conformance:

|            Option                          |                           Description                                        |
|--------------------------------------------|------------------------------------------------------------------------------|
| --mapping-table-pattern **pattern**        | A pattern to look for mapping table for the specified date.<br>The list of possible substitutions: `{0}` - year, `{1}` - month, `{2}` - day of month. By default the pattern is `reportDate={0}-{1}-{2}`. Special symbols in the pattern need to be escaped. For example, an empty pattern can be be specified as `\'\'` (single quotes are escaped using a backslash character).|
| --experimental-mapping-rule **true/false** | If `true`, the experimental optimized mapping rule implementation is used. The default value is build-specific and is set in 'application.properties'. |
| --catalyst-workaround **true/false**       | Turns on (`true`) or off (`false`) workaround for Catalyst optimizer issue. It is `true` by default. Turn this off only is you encounter timing freeze issues when running Conformance. | 
| --autoclean-std-folder **true/false**      | If `true`, the standardized folder will be cleaned automatically after successful execution of a Conformance job. |

## How to contribute
Please see our [**Contribution Guidelines**](CONTRIBUTING.md).

## Documentation
Please see the [documentation pages](https://absaoss.github.io/enceladus/).