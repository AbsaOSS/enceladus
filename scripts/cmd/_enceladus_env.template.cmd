:: Copyright 2018 ABSA Group Limited
::
:: Licensed under the Apache License, Version 2.0 (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
::     http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

:: Environment configuration
SET STD_HDFS_PATH=/bigdata/std/std-{0}-{1}-{2}-{3}

:: MongoDB connection configuration for Spline
:: Important! Special characters should be escaped using triple backslashes (\\\)
SET SPLINE_MONGODB_URL=mongodb://localhost:27017
SET SPLINE_MONGODB_NAME=spline

::SET SPARK_HOME="/opt/spark-2.4.4"
SET SPARK_SUBMIT="%SPARK_HOME%/bin/spark-submit"

SET HDP_VERSION=2.7.3

SET SPARK_JOBS_JAR=spark-jobs.jar

:: Environment-specific resource defaults for Standardization.
:: If empty and not specified explicitly, Spark configuration defaults will be used.
SET STD_DEFAULT_DRIVER_MEMORY=
SET STD_DEFAULT_DRIVER_CORES=
SET STD_DEFAULT_EXECUTOR_MEMORY=
SET STD_DEFAULT_EXECUTOR_CORES=
SET STD_DEFAULT_DRA_EXECUTOR_MEMORY=
SET STD_DEFAULT_DRA_EXECUTOR_CORES=
:: setting num executors disables DRA
SET STD_DEFAULT_NUM_EXECUTORS=

:: Dynamic Resource Allocation
:: also enables external shuffle service and adaptive execution for consistent setup
SET STD_DEFAULT_DRA_ENABLED=true
:: max executors limit is a required parameter
SET STD_DEFAULT_DRA_MAX_EXECUTORS=4

SET STD_DEFAULT_DRA_MIN_EXECUTORS=0
SET STD_DEFAULT_DRA_ALLOCATION_RATIO=0.5
SET STD_DEFAULT_ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE=134217728

:: Environment-specific resource defaults for Conformance.
:: If empty and not specified explicitly, Spark configuration defaults will be used.
SET CONF_DEFAULT_DRIVER_MEMORY=
SET CONF_DEFAULT_DRIVER_CORES=
SET CONF_DEFAULT_EXECUTOR_MEMORY=
SET CONF_DEFAULT_EXECUTOR_CORES=
SET CONF_DEFAULT_DRA_EXECUTOR_MEMORY=
SET CONF_DEFAULT_DRA_EXECUTOR_CORES=
:: setting num executors disables DRA
SET CONF_DEFAULT_NUM_EXECUTORS=

:: Dynamic Resource Allocation
:: also enables external shuffle service and adaptive execution for consistent setup
SET CONF_DEFAULT_DRA_ENABLED=true
:: max executors limit is a required parameter
SET CONF_DEFAULT_DRA_MAX_EXECUTORS=4

SET CONF_DEFAULT_DRA_MIN_EXECUTORS=0
SET CONF_DEFAULT_DRA_ALLOCATION_RATIO=0.5
SET CONF_DEFAULT_ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE=134217728

SET DEFAULT_DEPLOY_MODE=client

SET LOG_DIR=%TEMP%

:: Kafka security
:: Path to jaas.config
:: JAAS_CLIENT=-Djava.security.auth.login.config=/path/jaas.config
:: JAAS_CLUSTER=-Djava.security.auth.login.config=jaas_cluster.config

:: SET APPLICATION_PROPERTIES_CLIENT=-Dconfig.file=/absolute/path/application.conf
:: SET APPLICATION_PROPERTIES_CLUSTER=-Dconfig.file=application.conf

:: Files to send when running in cluster mode (comma separated)
:: Hash is used as the file alias: https://stackoverflow.com/a/49866757/1038282
:: SET ENCELADUS_FILES="/absolute/path/application.conf#application.conf"

:: Additional environment-specific Spark options, e.g. --conf "spark.driver.host=myhost"
:: To specify several configuration options prepend '--conf' to each config key.
:: Example: ADDITIONAL_SPARK_CONF=--conf "spark.driver.host=myhost" --conf "spark.driver.port=12233"
:: For secured HDFS the following two usually needs to be specified:
:: ADDITIONAL_SPARK_CONF=--conf spark.yarn.principal=<principal_name> --conf spark.yarn.keytab=<path_to_keytab>
SET ADDITIONAL_SPARK_CONF=

:: Additional JVM options
:: Example: ADDITIONAL_JVM_CONF=-Dtimezone=UTC -Dfoo=bar
:: for deployment mode: client
SET ADDITIONAL_JVM_CONF_CLIENT=%APPLICATION_PROPERTIES_CLIENT% %JAAS_CLIENT%

:: for deployment mode: cluster
SET ADDITIONAL_JVM_CONF_CLUSTER=%$APPLICATION_PROPERTIES_CLUSTER% %JAAS_CLUSTER%

SET MASTER=yarn

:: Switch that tells the script if it should exit if it encounters unrecognized.
:: On true it prints an Error and exits with 127, on false it only prints a warning
SET EXIT_ON_UNRECOGNIZED_OPTIONS=true

:: The way how the date is formatted in the OS, to check it run `echo %date%` on command line
:: parser1 for 25.06.2021
:: parser2 for Fri 06/25/2021
:: parser3 for 2021-06-25
:: The type of delimiters and language doesn't matter, parsing is done positionally
SET DATE_PARSING_TYPE=parser1
