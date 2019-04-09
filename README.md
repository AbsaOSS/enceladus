# Enceladus

[![Build Status](https://opensource.bigusdatus.com/jenkins/buildStatus/icon?job=Absa-OSS-Projects%2Fenceladus%2Fdevelop)](https://opensource.bigusdatus.com/jenkins/job/Absa-OSS-Projects/job/enceladus/job/develop/)
___

<!-- toc -->
- [What is Enceladus?](#what-is-enceladus)
    - [Menas](#menas)
    - [Standardization](#standardization)
    - [Conformance](#conformance)
- [How to build](#build)
- [How to run](#run)
- [How to contribute](#contribute)
<!-- tocstop -->

## <a name="what-is-enceladus"/>What is Enceladus?
**Enceladus** is a **Dynamic Conformance Engine** which allows data from different formats to be standardized to parquet and conformed to group-accepted common reference (e.g. data for country designation which are **DE** in one source system and **Deutschland** in another, can be conformed to **Germany**).

The project is comprised of three main components:
### <a name="menas"/>Menas
This is the user-facing web client, used to **specify the standardization schema**, and **define the steps required to conform** a dataset.  
There are three models used to do this:
 - **Dataset**: Specifies where the dataset will be read from on HDFS (**RAW**), the conformance rules that will be applied to it, and where it will land on HDFS once it is conformed (**PUBLISH**)
 - **Schema**: Specifies the schema towards which the dataset will be standardized
 - **Mapping Table**: Specifies where tables with master reference data can be found (parquet on HDFS), which are used when applying Mapping conformance rules (e.g. the dataset uses **Germany**, which maps to the master reference **DE** in the mapping table)

### <a name="standardization"/>Standardization
This is a Spark job which reads an input dataset in any of the supported formats and **produces a parquet dataset with the Menas-specified schema** as output. 

### <a name="conformance"/>Conformance
This is a Spark job which **applies the Menas-specified conformance rules to the standardized dataset**.

## <a name="build"/>How to build
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

## <a name="run"/>How to run
#### Menas requirements:
- **Tomcat 8.5/9.0** installation
- **MongoDB 4.0** installation
- **HADOOP_CONF_DIR** environment variable, pointing to the location of your hadoop configuration (pointing to a hadoop installation)

#### Deploying Menas
Simply copy the **menas.war** file produced when building the project into Tomcat's webapps directory. 

#### Standardization and Conformance requirements:
- **Spark 2.4.0** installation
- **Hadoop 2.7** installation
- **Menas** running instance
- **Menas Credentials File** in your home directory (a configuration file for authenticating the Spark jobs with Menas) 
e.g. `~/menas-credential.properties`:
```
username=user
password=changeme
```
 - **Directory structure** for the **RAW** dataset should follow the convention of `<path_to_dataset_in_menas>/<year>/<month>/<day>/v<dataset_version>`. This date is specified with the `--report-date` option when running the **Standardization** and **Conformance** jobs.
 - **_INFO file** must be present along with the **RAW** data on HDFS as per the above directory structure. This is a file tracking control measures via [Atum](https://github.com/AbsaOSS/atum), an example can be found [here](examples/data/input/_INFO).

#### Running Standardization
```
<spark home>/spark-submit \
--num-executors <num> \
--executor-memory <num>G \
--master yarn --deploy-mode <client/cluster> \
--driver-cores <num> \
--driver-memory <num>G \
--conf "spark.driver.extraJavaOptions=-Dmenas.rest.uri=<menas_api_uri:port> -Dstandardized.hdfs.path=<path_for_standardized_output>-{0}-{1}-{2}-{3} -Dspline.mongodb.url=<mongo_url_for_spline> -Dspline.mongodb.name=<spline_database_name> -Dhdp.version=<hadoop_version>" \
--class za.co.absa.enceladus.standardization.StandardizationJob \
<standardization_<build_version>.jar> \
--menas-credentials-file <path_to_menas_credentials> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <date> \
--report-version <data_run-version> \
--raw-format <data_format> \
--row-tag <tag>
```
* Here `row-tag` is a specific option for `raw-format` of type `XML`. For more options for different types please see our WIKI.

#### Running Conformance
```
<spark home>/spark-submit \
--num-executors <num> \
--executor-memory <num>G \
--master yarn --deploy-mode <client/cluster> \
--driver-cores <num> \
--driver-memory <num>G \
--conf 'spark.ui.port=29000' \
--conf "spark.driver.extraJavaOptions=-Dmenas.rest.uri=<menas_api_uri:port> -Dstandardized.hdfs.path=<path_of_standardized_input>-{0}-{1}-{2}-{3} -Dconformance.mappingtable.pattern=reportDate={0}-{1}-{2} -Dspline.mongodb.url=<mongo_url_for_spline> -Dspline.mongodb.name=<spline_database_name>" -Dhdp.version=<hadoop_version> \
--packages za.co.absa:enceladus-parent:<version>,za.co.absa:enceladus-conformance:<version> \
--class za.co.absa.enceladus.conformance.DynamicConformanceJob \
<conformance_<build_version>.jar> \
--menas-credentials-file <path_to_menas_credentials> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <date> \
--report-version <data_run-version>
```

## <a name="contribute"/>How to contribute
Please see our [**Contribution Guidelines**](CONTRIBUTING.md).
