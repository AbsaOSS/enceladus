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

# Command line for the script itself

# Show spark-submit command line without actually running it (--dry-run)
DRY_RUN=""

# Command line defaults for 'spark-submit'
MASTER="yarn"
DEPLOY_MODE="client"
NUM_EXECUTORS=""
EXECUTOR_MEMORY="4G"
DRIVER_CORES="4"
DRIVER_MEMORY="8G"

# Command like default for the job
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
TRIM_VALUES=""
MAPPING_TABLE_PATTERN=""
FOLDER_PREFIX=""
DEBUG_SET_RAW_PATH=""

# Security command line defaults
MENAS_CREDENTIALS_FILE=""
MENAS_AUTH_KEYTAB=""

# Parse command line (based on https://stackoverflow.com/questions/192249/how-do-i-parse-command-line-arguments-in-bash)
POSITIONAL=()
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
    --executor-memory)
    EXECUTOR_MEMORY="$2"
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
    --jar)
    JAR="$2"
    shift 2 # past argument and value
    ;;
    --class)
    JAR="$2"
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
    --trimValues)
    TRIM_VALUES="$2"
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

    --menas-credentials-file)
    MENAS_CREDENTIALS_FILE="$2"
    shift 2 # past argument and value
    ;;
    --menas-auth-keytab)
    MENAS_AUTH_KEYTAB="$2"
    shift 2 # past argument and value
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

# Display values of all declared variables
#declare -p

# Validation
VALID="1"
validate() {
    if [ -z "$1" ]; then
        echo "Missing mandatory option $2"
        VALID="0"
    fi
}

validate "$NUM_EXECUTORS" "--num-executors"
validate "$DATASET_NAME" "--dataset-name"
validate "$DATASET_VERSION" "--dataset-version"
validate "$REPORT_DATE" "--report-date"

if [[ "$MASTER" != "yarn" ]]; then
  echo "Master '$MASTER' is not allowed. The only allowed master is 'yarn'."
  VALID="0"
fi

# Validation failure check
if [ "$VALID" == "0" ]; then
    exit 1
fi

# Construct command line
CMD_LINE=""
add_to_cmd_line() {
    if [ ! -z "$2" ]; then
        CMD_LINE="$CMD_LINE $1 $2"
    fi
}

echoerr() {
    echo "$@" 1>&2;
}

# Constructing the grand command line
# Configuration passed to JVM

MT_PATTERN=""
if [ ! -z "$MAPPING_TABLE_PATTERN" ]; then
    MT_PATTERN="-Dconformance.mappingtable.pattern=$MAPPING_TABLE_PATTERN"
fi

CONF="spark.driver.extraJavaOptions=-Dmenas.rest.uri=$MENAS_URI -Dstandardized.hdfs.path=$STD_HDFS_PATH \
-Dspline.mongodb.url=$SPLINE_MONGODB_URL -Dspline.mongodb.name=$SPLINE_MONGODB_NAME -Dhdp.version=$HDP_VERSION \
$MT_PATTERN"

CMD_LINE="$SPARK_SUBMIT"

# Adding command line parameters that go BEFORE the jar file
add_to_cmd_line "--master" ${MASTER}
add_to_cmd_line "--deploy-mode" ${DEPLOY_MODE}
add_to_cmd_line "--num-executors" ${NUM_EXECUTORS}
add_to_cmd_line "--executor-memory" ${EXECUTOR_MEMORY}
add_to_cmd_line "--driver-cores" ${DRIVER_CORES}
add_to_cmd_line "--driver-memory" ${DRIVER_MEMORY}

# Adding JVM configuration, entry point class name and the jar file
CMD_LINE="$CMD_LINE --conf \"$CONF\" --class $CLASS $JAR"

# Adding command line parameters that go AFTER the jar file
add_to_cmd_line "--menas-auth-keytab" ${MENAS_AUTH_KEYTAB}
add_to_cmd_line "--menas-credentials-file" ${MENAS_CREDENTIALS_FILE}
add_to_cmd_line "--dataset-name" ${DATASET_NAME}
add_to_cmd_line "--dataset-version" ${DATASET_VERSION}
add_to_cmd_line "--report-date" ${REPORT_DATE}
add_to_cmd_line "--report-version" ${REPORT_VERSION}
add_to_cmd_line "--raw-format" ${RAW_FORMAT}
add_to_cmd_line "--charset" ${CHARSET}
add_to_cmd_line "--row-tag" ${ROW_TAG}
add_to_cmd_line "--delimiter" "${DELIMITER}"
add_to_cmd_line "--header" ${HEADER}
add_to_cmd_line "--csv-quote" ${CSV_QUOTE}
add_to_cmd_line "--csv-escape" ${CSV_ESCAPE}
add_to_cmd_line "--trimValues" ${TRIM_VALUES}
add_to_cmd_line "--folder-prefix" ${FOLDER_PREFIX}
add_to_cmd_line "--debug-set-raw-path" ${DEBUG_SET_RAW_PATH}

echo "Command line:"
echo "$CMD_LINE"

if [[ -z "$DRY_RUN" ]]; then
  if [[ "$DEPLOY_MODE" == "client" ]]; then
    DATE=`date +%Y_%m_%d-%H_%M_%S`
    NAME=`sed -e 's#.*\.\(\)#\1#' <<< $CLASS`
    TMP_PATH_NAME="$LOG_DIR/enceladus_${NAME}_${DATE}.log"

    # Initializing Kerberos ticket
    if [[ ! -z "$MENAS_AUTH_KEYTAB" ]]; then
      # Get principle stored in the keyfile (Thanks @Zejnilovic)
      PR=`printf "read_kt $MENAS_AUTH_KEYTAB\nlist" | ktutil | grep -Pio "(?<=\ )[A-Za-z0-9]*?(?=@)" | head -1`
      # Alternative way, might be less reliable
      # PR=`printf "read_kt $MENAS_AUTH_KEYTAB\nlist" | ktutil | sed -n '5p' | awk '{print $3}' | cut -d '@' -f1`
      if [[ ! -z "$PR" ]]; then
        # Initialize a ticket
        kinit -k -t "$MENAS_AUTH_KEYTAB" "$PR"
        klist 2>&1 | tee -a "$TMP_PATH_NAME"
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

    # Run the job
    bash -c "$CMD_LINE 2>&1 | tee -a $TMP_PATH_NAME"

    # Report the log location
    echo
    echo "Job has finished. The logs are saved to $TMP_PATH_NAME"
  else
    bash -c "$CMD_LINE"
  fi
fi
