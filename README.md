# Enceladus
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
**Enceladus** is a **Dynamic Conformance Engine** which allows data from different formats to be standardized to parquet and conformed to group-accepted common reference (e.g. **DE** in one source system and **Deutschland** in another, can be conformed to **Germany**).

The project is comprised of three main components:
### <a name="menas"/>Menas
This is the user-facing web client, used to **specify the standardization schema**, and **define the step required to conform** a dataset.  

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

#### Build command:

`mvn -DskipTests clean package`

## <a name="run"/>How to run
#### Menas requirements:
- **Tomcat 8.5/9.0** installation
- **MongoDB 4.0** installation
- **HADOOP_CONF_DIR** environment variable, pointing to the location of your hadoop configuration

#### Deploying Menas
Simply copy the **menas.war** file produced when building the project into Tomcat's webapps directory. 

#### Standardization and Conformance requirements:
- [**Spark 2.2.1-ABSA**](https://github.com/AbsaOSS/spark/tree/branch-2.2.1-ABSA) installation (this custom version can be [built like any other Spark version](https://spark.apache.org/docs/latest/building-spark.html))
- **Hadoop 2.7** installation
- **Menas Credentials File** (a configuration file containing a username and password for the Spark jobs to authenticate with Menas)

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
--menas-credentials-file <path_to_menas_credentials>
--dataset-name <dataset_name> \
--dataset-version <dataset_ersion> \
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
--menas-credentials-file <path_to_menas_credentials>
--dataset-name <dataset_name> \
--dataset-version <dataset_ersion> \
--report-date <date> \
--report-version <data_run-version>
```

## <a name="contribute"/>How to contribute
Please see our [**Contribution Guidelines**](https://github.com/AbsaOSS/enceladus/blob/develop/CONTRIBUTING.md).
