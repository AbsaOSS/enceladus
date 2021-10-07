#!/bin/bash

# Copyright 2018 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Environment configuration
STD_HDFS_PATH="/bigdata/std/std-{0}-{1}-{2}-{3}"

# MongoDB connection configuration for Spline
# Important! Special characters should be escaped using triple backslashes (\\\)
SPLINE_MONGODB_URL="mongodb://localhost:27017"
SPLINE_MONGODB_NAME="spline"

export SPARK_HOME="/opt/spark-2.4.4"
SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"

HDP_VERSION="2.7.3"

SPARK_JOBS_JAR="enceladus-spark-jobs.jar"

# Environment-specific resource defaults for Standardization.
# If empty and not specified explicitly, Spark configuration defaults will be used.
STD_DEFAULT_DRIVER_MEMORY=""
STD_DEFAULT_DRIVER_CORES=""
STD_DEFAULT_EXECUTOR_MEMORY=""
STD_DEFAULT_EXECUTOR_CORES=""
STD_DEFAULT_DRA_EXECUTOR_MEMORY=""
STD_DEFAULT_DRA_EXECUTOR_CORES=""
# setting num executors disables DRA
STD_DEFAULT_NUM_EXECUTORS=""

# Dynamic Resource Allocation
# also enables external shuffle service and adaptive execution for consistent setup
STD_DEFAULT_DRA_ENABLED=true
# max executors limit is a required parameter
STD_DEFAULT_DRA_MAX_EXECUTORS=4

STD_DEFAULT_DRA_MIN_EXECUTORS=0
STD_DEFAULT_DRA_ALLOCATION_RATIO=0.5
STD_DEFAULT_ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE=134217728

# Environment-specific resource defaults for Conformance.
# If empty and not specified explicitly, Spark configuration defaults will be used.
CONF_DEFAULT_DRIVER_MEMORY=""
CONF_DEFAULT_DRIVER_CORES=""
CONF_DEFAULT_EXECUTOR_MEMORY=""
CONF_DEFAULT_EXECUTOR_CORES=""
CONF_DEFAULT_DRA_EXECUTOR_MEMORY=""
CONF_DEFAULT_DRA_EXECUTOR_CORES=""
# setting num executors disables DRA
CONF_DEFAULT_NUM_EXECUTORS=""

# Dynamic Resource Allocation
# also enables external shuffle service and adaptive execution for consistent setup
CONF_DEFAULT_DRA_ENABLED=true
# max executors limit is a required parameter
CONF_DEFAULT_DRA_MAX_EXECUTORS=4

CONF_DEFAULT_DRA_MIN_EXECUTORS=0
CONF_DEFAULT_DRA_ALLOCATION_RATIO=0.5
CONF_DEFAULT_ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE=134217728

DEFAULT_DEPLOY_MODE="client"

LOG_DIR="/tmp"

DEFAULT_CLIENT_MODE_RUN_KINIT="true"

# Kafka security
# Path to jaas.config
#JAAS_CLIENT="-Djava.security.auth.login.config=/path/jaas.config"
#JAAS_CLUSTER="-Djava.security.auth.login.config=jaas_cluster.config"

APPLICATION_PROPERTIES_CLIENT="-Dconfig.file=/absolute/path/application.conf"
APPLICATION_PROPERTIES_CLUSTER="-Dconfig.file=application.conf"

#KRB5_CONF_CLIENT="-Djava.security.krb5.conf=/absolute/path/krb5.conf"
#KRB5_CONF_CLUSTER="-Djava.security.krb5.conf=krb5.conf"

#TRUST_STORE_CLIENT="-Djavax.net.ssl.trustStore=/absolute/path/trustStore.jks"
#TRUST_STORE_CLUSTER="-Djavax.net.ssl.trustStore=trustStore.jks"
#TRUST_STORE_PASSWORD="-Djavax.net.ssl.trustStorePassword=password"

# Files to send when running in cluster mode (comma separated)
# Hash is used as the file alias: https://stackoverflow.com/a/49866757/1038282
ENCELADUS_FILES="/absolute/path/application.conf#application.conf"
#ENCELADUS_FILES="${ENCELADUS_FILES},/absolute/path/krb5.conf#krb5.conf"
#ENCELADUS_FILES="${ENCELADUS_FILES},/absolute/path/emr_cacerts.jks#emr_cacerts.jks"

# Additional environment-specific Spark options, e.g. "--conf spark.driver.host=myhost"
# To specify several configuration options prepend '--conf' to each config key.
# Example: ADDITIONAL_SPARK_CONF="--conf spark.driver.host=myhost --conf spark.driver.port=12233"
# For secured HDFS the following two usually needs to be specified:
# ADDITIONAL_SPARK_CONF="--conf spark.yarn.principal=<principal_name> --conf spark.yarn.keytab=<path_to_keytab>"
ADDITIONAL_SPARK_CONF=""

# Additional JVM options
# Example: ADDITIONAL_JVM_CONF="-Dtimezone=UTC -Dfoo=bar"
# for deployment mode: client
ADDITIONAL_JVM_CONF_CLIENT="$APPLICATION_PROPERTIES_CLIENT $KRB5_CONF_CLIENT $TRUST_STORE_CLIENT $TRUST_STORE_PASSWORD $JAAS_CLIENT"
ADDITIONAL_JVM_EXECUTOR_CONF_CLIENT="$KRB5_CONF_CLIENT $TRUST_STORE_CLIENT $TRUST_STORE_PASSWORD"

# for deployment mode: cluster
# Warning!
# Avoid suppression of Info level logger. This will lead to the fact that, we are not able to get application_id
# and thus the scripts will not be able to continue properly, not giving the status update or kill option on interrupt
ADDITIONAL_JVM_CONF_CLUSTER="$APPLICATION_PROPERTIES_CLUSTER $KRB5_CONF_CLUSTER $TRUST_STORE_CLUSTER $TRUST_STORE_PASSWORD $JAAS_CLUSTER"
ADDITIONAL_JVM_EXECUTOR_CONF_CLUSTER="$KRB5_CONF_CLUSTER $TRUST_STORE_CLUSTER $TRUST_STORE_PASSWORD"

# Switch that tells the script if it should exit if it encounters unrecognized.
# On true it prints an Error and exits with 127, on false it only prints a warning
EXIT_ON_UNRECOGNIZED_OPTIONS="true"
