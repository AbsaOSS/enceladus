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

# Show spark-submit command line without actually running it (--dry-run)
DRY_RUN=""

# Command line defaults for 'spark-submit'
MASTER="yarn"
DEPLOY_MODE="$DEFAULT_DEPLOY_MODE"
DRIVER_CORES="$DEFAULT_DRIVER_CORES"
DRIVER_MEMORY="$DEFAULT_DRIVER_MEMORY"
EXECUTOR_CORES="$DEFAULT_EXECUTOR_CORES"
EXECUTOR_MEMORY="$DEFAULT_EXECUTOR_MEMORY"
DRA_EXECUTOR_CORES="$DEFAULT_DRA_EXECUTOR_CORES"
DRA_EXECUTOR_MEMORY="$DEFAULT_DRA_EXECUTOR_MEMORY"
NUM_EXECUTORS="$DEFAULT_NUM_EXECUTORS"
DRA_NUM_EXECUTORS=""
FILES="$ENCELADUS_FILES"

# DRA related defaults
DRA_ENABLED="$DEFAULT_DRA_ENABLED"

DRA_MIN_EXECUTORS="$DEFAULT_DRA_MIN_EXECUTORS"
DRA_MAX_EXECUTORS="$DEFAULT_DRA_MAX_EXECUTORS"
DRA_ALLOCATION_RATIO="$DEFAULT_DRA_ALLOCATION_RATIO"
ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE="$DEFAULT_ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE"

# Command like default for the job
JAR=${SPARK_JOBS_JAR_OVERRIDE:-$SPARK_JOBS_JAR}
DATASET_NAME=""
DATASET_VERSION=""
REPORT_DATE=""
REPORT_VERSION=""
RAW_FORMAT=""
CHARSET=""
ROW_TAG=""
DELIMITER=""
HEADER=""
CSV_QUOTE=""
CSV_ESCAPE=""
CSV_IGNORE_LEADING_WHITESPACE=""
CSV_IGNORE_TRAILING_WHITESPACE=""
TRIM_VALUES=""
EMPTY_VALUES_AS_NULLS=""
NULL_VALUE=""
COBOL_IS_TEXT=""
COBOL_ENCODING=""
IS_XCOM=""
COPYBOOK=""
COBOL_TRIMMING_POLICY=""
MAPPING_TABLE_PATTERN=""
FOLDER_PREFIX=""
DEBUG_SET_RAW_PATH=""
EXPERIMENTAL_MAPPING_RULE=""
CATALYST_WORKAROUND=""
AUTOCLEAN_STD_FOLDER=""
PERSIST_STORAGE_LEVEL=""
HELP_CALL="0"
ASYNCHRONOUSMODE="0"

# Spark configuration options
CONF_SPARK_EXECUTOR_MEMORY_OVERHEAD=""
CONF_SPARK_MEMORY_FRACTION=""

# Security command line defaults
MENAS_CREDENTIALS_FILE=""
MENAS_AUTH_KEYTAB=""
CLIENT_MODE_RUN_KINIT="$DEFAULT_CLIENT_MODE_RUN_KINIT"

# Parse command line (based on https://stackoverflow.com/questions/192249/how-do-i-parse-command-line-arguments-in-bash)
OTHER_PARAMETERS=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
  --dry-run)
    DRY_RUN="1"
    shift # past argument
    ;;
  --num-executors)
    NUM_EXECUTORS="$2"
    shift 2 # past argument and value
    ;;
  --dra-num-executors)
    DRA_NUM_EXECUTORS="$2"
    shift 2 # past argument and value
    ;;
  --executor-cores)
    EXECUTOR_CORES="$2"
    shift 2 # past argument and value
    ;;
  --executor-memory)
    EXECUTOR_MEMORY="$2"
    shift 2 # past argument and value
    ;;
  --dra-executor-cores)
    DRA_EXECUTOR_CORES="$2"
    shift 2 # past argument and value
    ;;
  --dra-executor-memory)
    DRA_EXECUTOR_MEMORY="$2"
    shift 2 # past argument and value
    ;;
  --master)
    MASTER="$2"
    shift 2 # past argument and value
    ;;
  --deploy-mode)
    DEPLOY_MODE="$2"
    shift 2 # past argument and value
    ;;
  --driver-cores)
    DRIVER_CORES="$2"
    shift 2 # past argument and value
    ;;
  --driver-memory)
    DRIVER_MEMORY="$2"
    shift 2 # past argument and value
    ;;
  --files)
    FILES="$ENCELADUS_FILES,$2"
    shift 2 # past argument and value
    ;;
  --conf-spark-executor-memoryOverhead)
    CONF_SPARK_EXECUTOR_MEMORY_OVERHEAD="$2"
    shift 2 # past argument and value
    ;;
  --conf-spark-memory-fraction)
    CONF_SPARK_MEMORY_FRACTION="$2"
    shift 2 # past argument and value
    ;;
  --jar)
    JAR="$2"
    shift 2 # past argument and value
    ;;
  --class)
    CLASS="$2"
    shift 2 # past argument and value
    ;;
  -D|--dataset-name)
    DATASET_NAME="$2"
    shift 2 # past argument and value
    ;;
  -d|--dataset-version)
    DATASET_VERSION="$2"
    shift 2 # past argument and value
    ;;
  -R|--report-date)
    REPORT_DATE="$2"
    shift 2 # past argument and value
    ;;
  -r|--report-version)
    REPORT_VERSION="$2"
    shift 2 # past argument and value
    ;;
  --folder-prefix)
    FOLDER_PREFIX="$2"
    shift 2 # past argument and value
    ;;
  -f|--raw-format)
    RAW_FORMAT="$2"
    shift 2 # past argument and value
    ;;
  --charset)
    CHARSET="$2"
    shift 2 # past argument and value
    ;;
  --row-tag)
    ROW_TAG="$2"
    shift 2 # past argument and value
    ;;
  --delimiter)
    if [[ "$2" == " " ]]; then
      DELIMITER="' '"
    else
      DELIMITER="\\$2"
    fi
    shift 2 # past argument and value
    ;;
  --header)
    HEADER="$2"
    shift 2 # past argument and value
    ;;
  --csv-quote)
    CSV_QUOTE="$2"
    shift 2 # past argument and value
    ;;
  --csv-escape)
    CSV_ESCAPE="$2"
    shift 2 # past argument and value
    ;;
  --csv-ignore-leading-white-space)
    CSV_IGNORE_LEADING_WHITESPACE="$2"
    shift 2 # past argument and value
    ;;
  --csv-ignore-trailing-white-space)
    CSV_IGNORE_TRAILING_WHITESPACE="$2"
    shift 2 # past argument and value
    ;;
  --trimValues)
    TRIM_VALUES="$2"
    shift 2 # past argument and value
    ;;
  --empty-values-as-nulls)
    EMPTY_VALUES_AS_NULLS="$2"
    shift 2 # past argument and value
    ;;
  --null-value)
    if [[ -z "${2// }" ]]; then
      NULL_VALUE="'$2'"
    else
      NULL_VALUE="$2"
    fi
    shift 2 # past argument and value
    ;;
  --cobol-encoding)
    COBOL_ENCODING="$2"
    shift 2 # past argument and value
    ;;
  --cobol-is-text)
    COBOL_IS_TEXT="$2"
    shift 2 # past argument and value
    ;;
  --cobol-trimming-policy)
    COBOL_TRIMMING_POLICY="$2"
    shift 2 # past argument and value
    ;;
  --is-xcom)
    IS_XCOM="$2"
    shift 2 # past argument and value
    ;;
  --copybook)
    COPYBOOK="$2"
    shift 2 # past argument and value
    ;;
  --mapping-table-pattern)
    MAPPING_TABLE_PATTERN="$2"
    shift 2 # past argument and value
    ;;
  --std-hdfs-path)
    STD_HDFS_PATH="$2"
    shift 2 # past argument and value
    ;;
  --debug-set-raw-path)
    DEBUG_SET_RAW_PATH="$2"
    shift 2 # past argument and value
    ;;
  --rest-api-credentials-file)
    MENAS_CREDENTIALS_FILE="$2"
    shift 2 # past argument and value
    ;;
  --rest-api-auth-keytab)
    MENAS_AUTH_KEYTAB="$2"
    shift 2 # past argument and value
    ;;
  --experimental-mapping-rule)
    EXPERIMENTAL_MAPPING_RULE="$2"
    shift 2 # past argument and value
    ;;
  --catalyst-workaround)
    CATALYST_WORKAROUND="$2"
    shift 2 # past argument and value
    ;;
  --autoclean-std-folder)
    AUTOCLEAN_STD_FOLDER="$2"
    shift 2 # past argument and value
    ;;
  --persist-storage-level)
    PERSIST_STORAGE_LEVEL="$2"
    shift 2 # past argument and value
    ;;
  --conf-spark-dynamicAllocation-minExecutors)
    DRA_MIN_EXECUTORS="$2"
    shift 2 # past argument and value
    ;;
  --conf-spark-dynamicAllocation-maxExecutors)
    DRA_MAX_EXECUTORS="$2"
    shift 2 # past argument and value
    ;;
  --conf-spark-dynamicAllocation-executorAllocationRatio)
    DRA_ALLOCATION_RATIO="$2"
    shift 2 # past argument and value
    ;;
  --conf-spark-sql-adaptive-shuffle-targetPostShuffleInputSize)
    ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE="$2"
    shift 2 # past argument and value
    ;;
  --set-dra)
    DRA_ENABLED="$2"
    shift 2 # past argument and value
    ;;
  --run-kinit)
    CLIENT_MODE_RUN_KINIT="$2"
    shift 2 # past argument and value
    ;;
   --min-processing-partition-size)
    MIN_PROCESSING_PARTITION_SIZE="$2"
    shift 2 # past argument and value
    ;;
  --max-processing-partition-size)
    MAX_PROCESSING_PARTITION_SIZE="$2"
    shift 2 # past argument and value
    ;;
  --help)
    HELP_CALL="1"
    shift # past argument
    ;;
  --asynchronous)
    ASYNCHRONOUSMODE="1"
    shift
    ;;
  *)    # unknown option
    OTHER_PARAMETERS+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done

too_many_options_print() {
  echo "$1: Found unrecognized options passed to the script. Parameters are:"
  echo "    $2"
  echo ""
}

if [[ -n "${OTHER_PARAMETERS[*]}" ]]; then
  if [[ "$EXIT_ON_UNRECOGNIZED_OPTIONS" == "true" ]]; then
    too_many_options_print "ERROR" "${OTHER_PARAMETERS[*]}"
    exit 127
  else
    too_many_options_print "WARNING" "${OTHER_PARAMETERS[*]}"
  fi
fi

# Display values of all declared variables
#declare -p

validate() {
    if [[ -z "$2" ]]; then
        echo "Missing mandatory option $1"
        VALID="0"
    fi
}

validate_either() {
    if [[ -z "$2" && -z "$4" ]]; then
        echo "Either $1 or $3 should be specified"
        VALID="0"
    fi
}

if [ "$HELP_CALL" == "0" ]; then
    # Validation (only if not help called)
    VALID="1"

    validate "--dataset-name" "$DATASET_NAME"
    validate "--dataset-version" "$DATASET_VERSION"
    validate "--report-date" "$REPORT_DATE"

    validate_either "--rest-api-credentials-file" "$MENAS_CREDENTIALS_FILE" "--rest-api-auth-keytab" "$MENAS_AUTH_KEYTAB"

    if [[ "$MASTER" != "yarn" ]]; then
      echo "Master '$MASTER' is not allowed. The only allowed master is 'yarn'."
      VALID="0"
    fi

    # Validation failure check
    if [ "$VALID" == "0" ]; then
        exit 1
    fi
fi

# Construct command line
add_to_cmd_line() {
    if [[ -n "$2" ]]; then
        CMD_LINE="$CMD_LINE $1 $2"
    fi
}

# Puts Spark configuration properties to the command line
add_spark_conf_cmd() {
    if [[ -n "$2" ]]; then
        SPARK_CONF="$SPARK_CONF --conf $1=$2"
    fi
}

echoerr() {
    echo "$@" 1>&2;
}

get_temp_log_file() {
    DATE=$(date +%Y_%m_%d-%H_%M_%S)
    NAME="${CLASS##*.}"
    TEMPLATE="enceladus_${NAME}_${DATE}_XXXXXX.log"

    mktemp -p "$LOG_DIR" -t "$TEMPLATE"
}

add_keytab_to_files() {
    MENAS_AUTH_KEYTAB_NAME=`echo "${MENAS_AUTH_KEYTAB}" | grep -o '[^/]*$'`
    FILES="${FILES},${MENAS_AUTH_KEYTAB}#${MENAS_AUTH_KEYTAB_NAME}"
    MENAS_AUTH_KEYTAB="${MENAS_AUTH_KEYTAB_NAME}"
}

CMD_LINE="$SPARK_SUBMIT"

# Constructing the grand command line
# Configuration passed to JVM

MT_PATTERN=""
if [ -n "$MAPPING_TABLE_PATTERN" ]; then
    MT_PATTERN="-Dconformance.mappingtable.pattern=$MAPPING_TABLE_PATTERN"
fi

MIN_PARTITION_SIZE=""
if [ -n "$MIN_PROCESSING_PARTITION_SIZE" ]; then
    MIN_PARTITION_SIZE="-Dmin.processing.partition.size=$MIN_PROCESSING_PARTITION_SIZE"
fi

MAX_PARTITION_SIZE=""
if [ -n "$MAX_PROCESSING_PARTITION_SIZE" ]; then
    MAX_PARTITION_SIZE="-Dmax.processing.partition.size=$MAX_PROCESSING_PARTITION_SIZE"
fi

SPARK_CONF="--conf spark.logConf=true"

# Dynamic Resource Allocation
# check DRA safe prerequisites
if [ "$DRA_ENABLED" = true ] && [ -z "$DRA_MAX_EXECUTORS" ] ; then
    echo "WARNING: maxExecutors should be set for Dynamic Resource Allocation. DRA is disabled"
    DRA_ENABLED=false
fi

# configure DRA and adaptive execution if enabled
if [ "$DRA_ENABLED" = true ] ; then
  echo "Dynamic Resource Allocation enabled"

  if [ -n "$NUM_EXECUTORS" ]; then
      echo "WARNING: num-executors should NOT be set when using Dynamic Resource Allocation and will be ignored."
      echo "Set dra-num-executors if you know what you are doing or disable dra"
  fi

  if [ -n "$EXECUTOR_MEMORY" ] || [ -n "$EXECUTOR_CORES" ]; then
    echo "Using values from --dra-executor-memory and --dra-executor-cores. --executor-memory and --executor-cores ignored"
  fi

  add_spark_conf_cmd "spark.dynamicAllocation.enabled" "true"
  add_spark_conf_cmd "spark.shuffle.service.enabled" "true"
  add_spark_conf_cmd "spark.sql.adaptive.enabled" "true"
  add_spark_conf_cmd "spark.dynamicAllocation.maxExecutors" "${DRA_MAX_EXECUTORS}"
  add_spark_conf_cmd "spark.dynamicAllocation.minExecutors" "${DRA_MIN_EXECUTORS}"
  add_spark_conf_cmd "spark.dynamicAllocation.executorAllocationRatio" "${DRA_ALLOCATION_RATIO}"
  add_spark_conf_cmd "spark.sql.adaptive.shuffle.targetPostShuffleInputSize" "${ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE}"

  add_to_cmd_line "--executor-memory" "${DRA_EXECUTOR_MEMORY}"
  add_to_cmd_line "--executor-cores" "${DRA_EXECUTOR_CORES}"
  add_to_cmd_line "--num-executors" "${DRA_NUM_EXECUTORS}"
else
  add_to_cmd_line "--executor-memory" "${EXECUTOR_MEMORY}"
  add_to_cmd_line "--num-executors" "${NUM_EXECUTORS}"
  add_to_cmd_line "--executor-cores" "${EXECUTOR_CORES}"
fi

JVM_CONF="spark.driver.extraJavaOptions=-Dstandardized.hdfs.path=$STD_HDFS_PATH \
-Dspline.mongodb.url=$SPLINE_MONGODB_URL -Dspline.mongodb.name=$SPLINE_MONGODB_NAME -Dhdp.version=$HDP_VERSION \
$MT_PATTERN $MIN_PARTITION_SIZE $MAX_PARTITION_SIZE"

if [ "$HELP_CALL" == "1" ]; then
  source ${SRC_DIR}/_print_help.sh
  exit "$?"
fi

if [[ "${MENAS_AUTH_KEYTAB}" =~ "^(s|S)3://.*" ]]; then
  echo "Using Keytab from S3"
  add_keytab_to_files
elif [[ -f "${MENAS_AUTH_KEYTAB}" ]]; then
  echo "Using Keytab from local FS"
  add_keytab_to_files
else
  echo "Using Keytab from HDFS"
fi

# Adding command line parameters that go BEFORE the jar file
add_to_cmd_line "--master" "${MASTER}"
add_to_cmd_line "--deploy-mode" "${DEPLOY_MODE}"
add_to_cmd_line "--driver-cores" "${DRIVER_CORES}"
add_to_cmd_line "--driver-memory" "${DRIVER_MEMORY}"
add_to_cmd_line "--files" "${FILES}"

# Adding Spark config options
add_spark_conf_cmd "spark.executor.memoryOverhead" "${CONF_SPARK_EXECUTOR_MEMORY_OVERHEAD}"
add_spark_conf_cmd "spark.memory.fraction" "${CONF_SPARK_MEMORY_FRACTION}"

# Adding JVM configuration, entry point class name and the jar file
if [[ "$DEPLOY_MODE" == "client" ]]; then
  ADDITIONAL_JVM_CONF="$ADDITIONAL_JVM_CONF_CLIENT"
  ADDITIONAL_JVM_EXECUTOR_CONF="$ADDITIONAL_JVM_EXECUTOR_CONF_CLIENT"
else
  ADDITIONAL_JVM_CONF="$ADDITIONAL_JVM_CONF_CLUSTER"
  ADDITIONAL_JVM_EXECUTOR_CONF="$ADDITIONAL_JVM_EXECUTOR_CONF_CLUSTER"
  add_spark_conf_cmd "spark.yarn.submit.waitAppCompletion" "false"
fi

CMD_LINE="${CMD_LINE} ${ADDITIONAL_SPARK_CONF} ${SPARK_CONF}"
CMD_LINE="${CMD_LINE} --conf \"${JVM_CONF} ${ADDITIONAL_JVM_CONF}\""
CMD_LINE="${CMD_LINE} --conf \"spark.executor.extraJavaOptions=${ADDITIONAL_JVM_EXECUTOR_CONF}\""
CMD_LINE="${CMD_LINE} --class ${CLASS} ${JAR}"

# Adding command line parameters that go AFTER the jar file
add_to_cmd_line "--rest-api-auth-keytab" "${MENAS_AUTH_KEYTAB}"
add_to_cmd_line "--rest-api-credentials-file" "${MENAS_CREDENTIALS_FILE}"
add_to_cmd_line "--dataset-name" "${DATASET_NAME}"
add_to_cmd_line "--dataset-version" "${DATASET_VERSION}"
add_to_cmd_line "--report-date" "${REPORT_DATE}"
add_to_cmd_line "--report-version" "${REPORT_VERSION}"
add_to_cmd_line "--raw-format" "${RAW_FORMAT}"
add_to_cmd_line "--charset" "${CHARSET}"
add_to_cmd_line "--row-tag" "${ROW_TAG}"
add_to_cmd_line "--delimiter" "${DELIMITER}"
add_to_cmd_line "--header" "${HEADER}"
add_to_cmd_line "--csv-quote" "${CSV_QUOTE}"
add_to_cmd_line "--csv-escape" "${CSV_ESCAPE}"
add_to_cmd_line "--csv-ignore-leading-white-space" "${CSV_IGNORE_LEADING_WHITESPACE}"
add_to_cmd_line "--csv-ignore-trailing-white-space" "${CSV_IGNORE_TRAILING_WHITESPACE}"
add_to_cmd_line "--trimValues" "${TRIM_VALUES}"
add_to_cmd_line "--empty-values-as-nulls" "${EMPTY_VALUES_AS_NULLS}"
add_to_cmd_line "--null-value" "${NULL_VALUE}"
add_to_cmd_line "--cobol-is-text" "${COBOL_IS_TEXT}"
add_to_cmd_line "--cobol-encoding" "${COBOL_ENCODING}"
add_to_cmd_line "--cobol-trimming-policy" "${COBOL_TRIMMING_POLICY}"
add_to_cmd_line "--is-xcom" "${IS_XCOM}"
add_to_cmd_line "--copybook" "${COPYBOOK}"
add_to_cmd_line "--folder-prefix" "${FOLDER_PREFIX}"
add_to_cmd_line "--debug-set-raw-path" "${DEBUG_SET_RAW_PATH}"
add_to_cmd_line "--experimental-mapping-rule" "${EXPERIMENTAL_MAPPING_RULE}"
add_to_cmd_line "--catalyst-workaround" "${CATALYST_WORKAROUND}"
add_to_cmd_line "--autoclean-std-folder" "${AUTOCLEAN_STD_FOLDER}"
add_to_cmd_line "--persist-storage-level" "${PERSIST_STORAGE_LEVEL}"

echo "Command line:"
echo "$CMD_LINE"

if [[ -z "$DRY_RUN" ]]; then
  if [[ "$DEPLOY_MODE" == "client" ]]; then
    TMP_PATH_NAME=$(get_temp_log_file)
    # Initializing Kerberos ticket
    if [[ -n "$MENAS_AUTH_KEYTAB" ]] && [[ "$CLIENT_MODE_RUN_KINIT" == "true" ]]; then
      # Get principle stored in the keyfile
      PR=$(printf "read_kt %s\nlist" "$MENAS_AUTH_KEYTAB" | ktutil | grep -Pio "(?<=\ )[A-Za-z0-9\-\._]*?(?=@)" | head -1)
      # Alternative way, might be less reliable
      # PR=$(printf "read_kt $MENAS_AUTH_KEYTAB\nlist" | ktutil | sed -n '5p' | awk '{print $3}' | cut -d '@' -f1)
      if [[ -n "$PR" ]]; then
        # Initialize a ticket
        kinit -k -t "$MENAS_AUTH_KEYTAB" "$PR"
        klist -e 2>&1 | tee -a "$TMP_PATH_NAME"
      else
        echoerr "WARNING!"
        echoerr "Unable to determine principle from the keytab file $MENAS_AUTH_KEYTAB."
        echoerr "Please make sure Kerberos ticket is initialized by running 'kinit' manually."
        sleep 10
      fi
    fi
    # Log the log location
    echo "$CMD_LINE" >> "$TMP_PATH_NAME"
    echo "The log will be saved to $TMP_PATH_NAME"
    # Run the job and return exit status of the last failed command in the subshell pipeline (Issue #893)
    set +e
    bash -c "set -o pipefail; $CMD_LINE 2>&1 | tee -a $TMP_PATH_NAME"
    # Save the exit status of spark submit subshell run
    EXIT_STATUS="$?"
    # Test if the command executed successfully
    if [ $EXIT_STATUS -eq 0 ]; then
      RESULT="passed"
    else
      RESULT="failed"
    fi
    # Report the result and log location
    echo ""
    echo "Job $RESULT with exit status $EXIT_STATUS. Refer to logs at $TMP_PATH_NAME" | tee -a "$TMP_PATH_NAME"
    exit $EXIT_STATUS
  else
    # Kills the yarn application. Invoked by the trap on EXIT
    function kill_yarn_app {
      yarn application -kill "$APPLICATIONID"
    }

    APPLICATIONID=$(bash -c "$CMD_LINE" 2>&1 | grep -oP "(?<=Submitted application ).*" )

    if [ -z "$APPLICATIONID" ]; then
      echo "Failed to capture the Application ID. Exiting."
      exit 1
    else
      echo "Application Id : $APPLICATIONID"
    fi

    if [ "$ASYNCHRONOUSMODE" == "1" ]; then
      echo "Running in asynchronous mode. Script exiting. Application ID can be found above"
      exit 0
    fi

    # If EXIT is invoked while the script is running, this will invoke kill_yarn_app function
    trap kill_yarn_app EXIT

    STATE='NOT FINISHED'

    echo "State: Application Started"
    while [[ "$STATE" != "FINISHED" && "$STATE" != "FAILED" && "$STATE" != "KILLED" ]];  do
      sleep 30
      STATE=$(yarn application -status "$APPLICATIONID" | grep -oP "(?<=\sState : ).*" )
      echo State: "$STATE"
    done

    FINALSTATE=$(yarn application -status "$APPLICATIONID" | grep -oP "(?<=\sFinal-State : ).*" )

    echo "$FINALSTATE"
    case "$FINALSTATE" in
      "SUCCEEDED" )
        exit 0
        ;;
      "KILLED" )
        exit 130
        ;;
      * )
        exit 1
        ;;
    esac
  fi
fi
