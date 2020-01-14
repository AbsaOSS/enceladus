#!/bin/bash

# Copyright 2018-2019 ABSA Group Limited
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

# The Menas URI can specify multiple semi-colon-separated base URIs
# each can have multiple comma-separated hosts, these are used for fault-tolerance
MENAS_URI="http://localhost:8080/menas;http://remotehost:8080/menas"
STD_HDFS_PATH="/bigdata/std/std-{0}-{1}-{2}-{3}"

# MongoDB connection configuration for Spline
# Important! Special characters should be escaped using triple backslashes (\\\)
SPLINE_MONGODB_URL="mongodb://localhost:27017"
SPLINE_MONGODB_NAME="spline"

export SPARK_HOME="/opt/spark-2.4.4"
SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"

HDP_VERSION="2.7.3"

ENCELADUS_JAR="enceladus-conformance.jar"

STD_CLASS="za.co.absa.enceladus.standardization.StandardizationJob"

# Environment-specific resource defaults for Standardization.
# If empty and not specified explicitly, Spark configuration defaults will be used.
STD_DEFAULT_DRIVER_MEMORY=""
STD_DEFAULT_DRIVER_CORES=""
STD_DEFAULT_EXECUTOR_MEMORY=""
STD_DEFAULT_EXECUTOR_CORES=""
STD_DEFAULT_NUM_EXECUTORS=""

CONF_CLASS="za.co.absa.enceladus.conformance.DynamicConformanceJob"

# Environment-specific resource defaults for Conformance.
# If empty and not specified explicitly, Spark configuration defaults will be used.
CONF_DEFAULT_DRIVER_MEMORY=""
CONF_DEFAULT_DRIVER_CORES=""
CONF_DEFAULT_EXECUTOR_MEMORY=""
CONF_DEFAULT_EXECUTOR_CORES=""
CONF_DEFAULT_NUM_EXECUTORS=""

DEFAULT_DEPLOY_MODE="client"

LOG_DIR="/tmp"

# Additional environment-specific Spark options, e.g. "--conf spark.driver.host=myhost"
# To specify several configuration options prepend '--conf' to each config key.
# Example: ADDITIONAL_SPARK_CONF="--conf spark.driver.host=myhost --conf spark.driver.port=12233"
ADDITIONAL_SPARK_CONF=""

# Additional JVM options
# Example: ADDITIONAL_JVM_CONF="-Dtimezone=UTC -Dfoo=bar"
ADDITIONAL_JVM_CONF=""
