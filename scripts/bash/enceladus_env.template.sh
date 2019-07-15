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
MENAS_URI="http://localhost:8080/menas/api"
STD_HDFS_PATH="/bigdata/std/std-{0}-{1}-{2}-{3}"

# MongoDB connection configuration for Spline
# Important! Special characters should be escaped using triple backslashes (\\\)
SPLINE_MONGODB_URL="mongodb://localhost:27017"
SPLINE_MONGODB_NAME="spline"

export SPARK_HOME="/opt/spark-2.4.3"
SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"

STD_CLASS="za.co.absa.enceladus.standardization.StandardizationJob"
STD_JAR="enceladus-standardization.jar"

CONF_CLASS="za.co.absa.enceladus.conformance.DynamicConformanceJob"
CONF_JAR="enceladus-conformance.jar"

LOG_DIR="/tmp"
