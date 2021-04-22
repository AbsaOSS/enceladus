@ECHO OFF

:: Copyright 2018 ABSA Group Limited
::
:: Licensed under the Apache License, Version 2.0 (the %License%);
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
::     http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an %AS IS% BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.

ECHO Enceladus Helper Scripts
ECHO/
ECHO Usage: run_[job-name].cmd [script-specific-options] [spark-specific-options] [job-specific-options]
ECHO/
ECHO job-name:
ECHO   standardization                 To run a Standardization only script
ECHO   conformance                     To run a Conformance only script
ECHO   standardization_conformance     To run a joint Standardization and Conformance script
ECHO/
ECHO script-specific-options:
ECHO   --help             To print this message
ECHO   --asynchronous     To run the job in an asynchronous mode. The script will exit after launching the job. Works only in `cluster` deploy mode
ECHO   --dry-run          Show spark-submit command line without actually running it
ECHO/
ECHO spark-specific-options:
ECHO !!!WARNING - set these only if you know what you are doing. This could for example disable DRA
ECHO   --num-executors NUM                      Number of executors to launch. Effective only if DRA is off.
ECHO   --executor-cores NUM                     Number of cores per executor. Effective only if DRA is off.
ECHO   --executor-memory MEM                    Memory per executor (e.g. 1000M, 2G) (Default: 1G). Effective only if DRA is off.
ECHO   --dra-num-executors NUM                  Same as '--num-executors' but used when DRA is enabled. Use with care! DRA won't scale below this NUM.
ECHO   --dra-executor-cores NUM                 Same as '--executor-memory' but used when DRA is enabled.
ECHO   --dra-executor-memory MEM                Same as '--executor-cores' but used when DRA is enabled.
ECHO   --master MASTER_URL                      spark://host:port, mesos://host:port, yarn, k8s://https://host:port, or local
ECHO   --deploy-mode DEPLOY_MODE                Whether to launch the driver program locally ("client") or on one of the worker machines inside the cluster ("cluster").
ECHO   --driver-cores NUM                       Number of cores used by the driver, only in cluster mode.
ECHO   --driver-memory                          Memory for driver (e.g. 1000M, 2G)
ECHO   --files FILES                            Comma-separated list of files to be placed in the working directory of each executor.
ECHO   --conf-spark-executor-memoryOverhead     Amount of non-heap memory to be allocated per driver process in cluster mode, in MiB unless otherwise specified.
ECHO   --conf-spark-memory-fraction NUM         Fraction of the heap space reserved for execution and storage regions (default 0.6)
ECHO   --jar                                    Custom path to Enceladus's SparkJobs jar
ECHO   --class CLASS_NAME                       Application's main class.
ECHO/
ECHO job-specific-options:
ECHO   Running the JAR --help to print all job specific options

SET HELP_CONF_DRIVER=spark.driver.extraJavaOptions=-Dlog4j.rootCategory='WARN, console'
SET HELP_CONF_EXECUTOR=spark.executor.extraJavaOptions=-Dlog4j.rootCategory='WARN, console'
SET HELP_SPARK_BASE=%SPARK_SUBMIT% --deploy-mode client
SET HELP_CMD=%HELP_SPARK_BASE% --conf "%HELP_CONF_DRIVER%" --conf "%HELP_CONF_EXECUTOR%" --class %CLASS% %JAR% --help

%HELP_CMD%
