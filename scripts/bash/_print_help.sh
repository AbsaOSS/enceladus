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

# Command line for the script itself

set -e

echo "Enceladus Helper Scripts"
echo ""
echo "Usage: run_[job-name].sh [script-specific-options] [spark-specific-options] [job-specific-options]"
echo ""
echo "job-name:"
echo "  standardization                 To run a Standardization only script"
echo "  conformance                     To run a Conformance only script"
echo "  standardization_conformance     To run a joint Standardization and Conformance script"
echo ""
echo "script-specific-options:"
echo "  --help             To print this message"
echo "  --asynchronous     To run the job in an asynchronous mode. The script will exit after printing application ID. Cluster mode only."
echo "  --dry-run          Show spark-submit command line without actually running it"
echo ""
echo "spark-specific-options:"
echo "!!!WARNING - set these only if you know what you are doing. This could for example disable DRA"
echo "  --num-executors NUM                      Number of executors to launch."
echo "  --executor-cores NUM                     Number of cores per executor."
echo "  --executor-memory MEM                    Memory per executor (e.g. 1000M, 2G) (Default: 1G)."
echo "  --master MASTER_URL                      spark://host:port, mesos://host:port, yarn, k8s://https://host:port, or local"
echo "  --deploy-mode DEPLOY_MODE                Whether to launch the driver program locally (\"client\") or on one of the worker machines inside the cluster (\"cluster\")."
echo "  --driver-cores NUM                       Number of cores used by the driver, only in cluster mode."
echo "  --driver-memory                          Memory for driver (e.g. 1000M, 2G)"
echo "  --files FILES                            Comma-separated list of files to be placed in the working directory of each executor."
echo "  --conf-spark-executor-memoryOverhead     Amount of non-heap memory to be allocated per driver process in cluster mode, in MiB unless otherwise specified."
echo "  --conf-spark-memory-fraction NUM         Fraction of the heap space reserved for execution and storage regions (default 0.6)"
echo "  --jar                                    Custom path to Enceladus's SparkJobs jar"
echo "  --class CLASS_NAME                       Application's main class."
echo ""
echo "job-specific-options:"
echo "  Running the JAR --help to print all job specific options"

HELP_CMD="$SPARK_SUBMIT $HELP_SPARK_BASE --class $CLASS $JAR --help"

bash -c "set -o pipefail; $HELP_CMD 2>&1"
