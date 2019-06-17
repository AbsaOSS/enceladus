# Enceladus

### <a name="build_status"/>Build Status
| develop |
| ------------- |
| [![Build Status](https://opensource.bigusdatus.com/jenkins/buildStatus/icon?job=Absa-OSS-Projects%2Fenceladus%2Fdevelop)](https://opensource.bigusdatus.com/jenkins/job/Absa-OSS-Projects/job/enceladus/job/develop/)  | 

___

<!-- toc -->
- [What is Enceladus?](#what-is-enceladus)
    - [Menas](#wie-menas)
    - [Standardization](#wie-standardization)
    - [Conformance](#wie-conformance)
- [How to build](#build)
- [How to run](#run)
- [How to contribute](#contribute)
- [How to use](#use)
    - [Standardization](#use-standardization)
<!-- tocstop -->

## <a name="what-is-enceladus"/>What is Enceladus?
**Enceladus** is a **Dynamic Conformance Engine** which allows data from different formats to be standardized to parquet and conformed to group-accepted common reference (e.g. data for country designation which are **DE** in one source system and **Deutschland** in another, can be conformed to **Germany**).

The project is comprised of three main components:
### <a name="wie-menas"/>Menas
This is the user-facing web client, used to **specify the standardization schema**, and **define the steps required to conform** a dataset.  
There are three models used to do this:
 - **Dataset**: Specifies where the dataset will be read from on HDFS (**RAW**), the conformance rules that will be applied to it, and where it will land on HDFS once it is conformed (**PUBLISH**)
 - **Schema**: Specifies the schema towards which the dataset will be standardized
 - **Mapping Table**: Specifies where tables with master reference data can be found (parquet on HDFS), which are used when applying Mapping conformance rules (e.g. the dataset uses **Germany**, which maps to the master reference **DE** in the mapping table)

### <a name="wie-standardization"/>Standardization
This is a Spark job which reads an input dataset in any of the supported formats and **produces a parquet dataset with the Menas-specified schema** as output. 

### <a name="wie-conformance"/>Conformance
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
- With component preload file generated (requires npm):  `mvn clean package -PgenerateComponentPreload`

## <a name="run"/>How to run
#### Menas requirements:
- **Tomcat 8.5/9.0** installation
- **MongoDB 4.0** installation
- **HADOOP_CONF_DIR** environment variable, pointing to the location of your hadoop configuration (pointing to a hadoop installation)

#### Deploying Menas
Simply copy the **menas.war** file produced when building the project into Tomcat's webapps directory. 

#### Speed up initial loading time of menas
- Build the project with the generateComponentPreload profile. Component preload will greatly reduce the number of HTTP requests required for the initial load of Menas
- Enable the HTTP compression
- Configure `spring.resources.cache.cachecontrol.max-age` in `application.properties` of Menas for caching of static resources

#### Standardization and Conformance requirements:
- **Spark 2.4.3 (Scala 2.11)** installation
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

## <a name="use"/>How to use
In this section some more complex and less obvious usage patterns are going to be described.

### <a name="use-standardization"/>Standardization
#### Adjusting Standardization Of Data
_Standardization_ can be influenced by `metadata` in the schema of the data. These are the possible properties taken
into account with the description of their purpose.

| Property | Target data type | Description | Example |
| --- | --- | --- | --- |
| _sourcecolumn_ | any | The source column to provide data of the described column | _id_ |
| _default_ | any atomic type| Default value to use in case data are missing | _0_ |
| _pattern_ | date & timestamp | Pattern for the date or timestamp representation | _dd.MM.yy_ |
| _timezone_ | timestamp (also date) | The time zone of the timestamp when that is not part of the pattern (NB! for date it can return unexpected results) | _US/Pacific_ |

Schema entry example:
```
{
    "name": "MODIFIEDTIMESTAMP",
    "type": "timestamp",
    "nullable": true,
    "metadata": {    
        "description": "Timestamp when the row was last changed.",
        "sourcecolumn": "MODIFIED"
        "default": "1970/01/01 01-00-00"
        "pattern": "yyyy/MM/dd HH-mm-ss"
        "timezone": "CET"
    }
}
  ```

#### Date & time
Dates and especially timestamps (date + time) can be tricky. Currently Spark considers all time entries to be in the 
current system time zone by default. (For more detailed explanation of possible issues with that see 
[Consistent timestamp types in Hadoop SQL engines](https://docs.google.com/document/d/1gNRww9mZJcHvUDCXklzjFEQGpefsuR_akCDfWsdE35Q/edit#heading=h.n699ftkvhjlo).)

To address this potential source of discrepancies the following has been implemented:
1. All Enceladus components are set to run in UTC
1. As part of **Standardization** all time related entries are normalized to UTC
1. There are several methods how to ensure that a timestamp entry is normalized as expected
1. We urge users, that all timestamp entries should include time zone information in one of the supported ways
1. While this is all valid for date entries too, it should be noted that UTC normalization of a date can have unexpected 
consequences - namely all dates west from UTC would be shifted to a day earlier

##### Date & timestamp pattern
To enable processing of time entries from other systems **Standardization** offers the possibility to convert 
string and even numeric values to timestamp or date types. It's done using Spark's ability to convert strings to 
timestamp/date with some enhancements. The pattern placeholders and usage is described in Java's 
[`SimpleDateFormat` class description](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html) with 
the addition of recognizing two keywords `epoch` and `milliepoch` (case insensitive) to denote the number of 
seconds/milliseconds since epoch (1970/01/01 00:00:00.000 UTC).
It should be noted explicitly that `epoch` and `milliepoch` are considered a pattern including time zone.
 
Summary:

| placeholder | Description | Example |
| --- | --- | --- |
| G | Era designator | AD |
| y | Year | 1996; 96 |
| Y | Week year | 2009; 09 |
| M | Month in year (context sensitive) |  July; Jul; 07 |
| L | Month in year (standalone form) | July; Jul; 07 |
| w | Week in year | 27 |
| W | Week in month | 2 |
| D | Day in year | 189 |
| d | Day in month |  10 |
| F | Day of week in month | 2 |
| E | Day name in week | Tuesday; Tue |
| u | Day number of week (1 = Monday, ..., 7 = Sunday) | 1 |
| a | Am/pm marker | PM |
| H | Hour in day (0-23) | 0 |
| k | Hour in day (1-24) | 24 |
| K | Hour in am/pm (0-11) |  0 |
| h | Hour in am/pm (1-12) | 12 |
| m | Minute in hour | 30 |
| s | Second in minute | 55 |
| S | Millisecond | 978 |
| z | General time zone | Pacific Standard Time; PST; GMT-08:00 |
| Z | RFC 822 time zone | -0800 |
| X | ISO 8601 time zone | -08; -0800; -08:00 |
| _epoch_ | Seconds since 1970/01/01 00:00:00 | 1557136493|
| _milliepoch_ | Milliseconds since 1970/01/01 00:00:00.0000| 15571364938124 |

**NB!** Spark uses US Locale and because on-the-fly conversion would be complicated, at the moment we stick to this 
hardcoded locale as well. E.g. `am/pm` for `a` placeholder, English names of days and months etc.

**NB!** The keywords are case **insensitive**. Therefore, there is no difference between `epoch` and `EpoCH`.
   
##### Time Zone support
As it has been mentioned, it's highly recommended to use timestamps with the time zone. But it's not unlikely that the 
source for standardization doesn't provide the time zone information. On the other hand, these times are usually within
one time zone. To ensure proper standardization, the schema's _metadata_ can include the `timezone` value.
All timestamps then will be standardized as belonging to the particular time zone.  

E.g. _2019-05-04 11:31:10_ with `timzene` specified as _CET_ will be standardized to _2019-05-04 10:31:10_ (UTC of 
course)

In case the pattern already includes information to recognize the time zone, the `timezone` entry in _metadata_ will 
be ignored. Namely if the pattern includes 'z', 'Z' or 'X' placeholder or `epoch`/`milliepoch` keywords.

**NB!** Due to spark limitation, only time zone ids are accepted as valid values. To get the full list of supported time
 zone denominators see the output of Java's 
[`TimeZone.getAvailableIDs()` function](https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html#getAvailableIDs--). 

##### Default value
Default value is used to handle **NULL** values in non-nullable columns when they are being standardized. This can be due to type mismatch or **NULL** entries.
Date and timestamp default values, specifically, have to adhere to the provided `pattern`. If no pattern is 
provided, the implicit pattern is used - `yyyy-MM-dd` for dates and `yyyy-MM-dd HH:mm:ss` for timestamps.
