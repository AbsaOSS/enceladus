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

# We have 2 states.
#   Write Standardisation
#   Write Conformance
# The script will have to figure out which is being run and if it is being written to S3 or HDFS.
#   In case of standardisation parsing the java properties supplied to figure out the standardisation path.
#   In case of conformance calling Menas for info over REST API using supplied metadata
# Then cleaning up the paths to conform to the path schema in query provided.

# TODO - Auto load jceks files

set -e

# Source env variables
source "$(dirname "$0")/enceladus_env.sh"

# Initialize variables
keytab=""
dataset_name=""
dataset_version=""
report_date=""
report_version=""

# The first argument is the name of the original script
original_script="$(dirname "$0")/$(basename "$1")"

# Shift the first argument so we can process the rest
shift

# Check if the original script exists
if [[ ! -f "$original_script" ]]; then
  echo "Error: The script '$original_script' does not exist in the current directory."
  exit 1
fi

# Initialize an array to hold the other arguments
other_args=()

# Loop through arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --menas-auth-keytab)
      keytab="$2"
      shift # past argument
      shift # past value
      ;;
    --dataset-name)
      dataset_name="$2"
      shift # past argument
      shift # past value
      ;;
    --dataset-version)
      dataset_version="$2"
      shift # past argument
      shift # past value
      ;;
    --report-date)
      report_date="$2"
      shift # past argument
      shift # past value
      ;;
    --report-version)
      report_version="$2"
      shift # past argument
      shift # past value
      ;;
    *)    # unknown option
      other_args+=("$1") # save it in an array for later
      shift # past argument
      ;;
  esac
done

# Print the extracted variables
echo "Keytab: $keytab"
echo "Dataset Name: $dataset_name"
echo "Dataset Version: $dataset_version"
echo "Report Date: $report_date"
echo "Report Version: $report_version"

# Run klist to check for a current Kerberos ticket
if klist -s; then
    echo "Kerberos ticket found."
else
    echo "No Kerberos ticket found or ticket is expired. Please run kinit."
fi

# Get Dataset info
response=$(curl --negotiate -s -u : "$MENAS_API/dataset/$dataset_name/$dataset_version")
if [ $? -ne 0 ]; then
    echo "Could not load dataset info - $dataset_name v $dataset_version from Menas at $MENAS_API"
    exit 1
fi

# Parse the response using jq to extract hdfsPublishPath and hdfsPath
hdfsPublishPath=$(echo "$response" | jq -r '.hdfsPublishPath')
hdfsPath=$(echo "$response" | jq -r '.hdfsPath')

# Check if the paths are null or not
if [[ $hdfsPublishPath != "null" && $hdfsPath != "null" ]]; then
    echo "hdfsPublishPath: $hdfsPublishPath"
    echo "hdfsPath: $hdfsPath"
else
    echo "Could not find the required paths in the response."
    exit 1
fi

# Run the original script with all the arguments
"$original_script" "${other_args[@]}" \
  --menas-auth-keytab "$keytab" \
  --dataset-name "$dataset_name" \
  --dataset-version "$dataset_version" \
  --report-date "$report_date" \
  --report-version "$report_version"

# Save the exit code
exit_code=$?

# Run versions cleanup for publish on s3a
if [[ $hdfsPublishPath == s3a://* ]]; then
  echo "We have publish versions to clean:"
  curl -X GET \
    --header "x-api-key: $ECS_API_KEY" \
    -d "{\"ecs_path\":\"${hdfsPublishPath#s3a://}\"}" \
    $ECS_API_KK
  echo
  curl -X DELETE \
    --header "x-api-key: $ECS_API_KEY" \
    -d "{\"ecs_path\":\"${hdfsPublishPath#s3a://}\"}" \
    $ECS_API_KK
  echo
  echo "Versions cleaned"
else
  echo "No publish versions to clean."
fi

if [[ $STD_HDFS_PATH == s3a://* ]]; then
  STD_HDFS_PATH_FILLED="${STD_HDFS_PATH//\{0\}/$dataset_name}"
  STD_HDFS_PATH_FILLED="${STD_HDFS_PATH_FILLED//\{1\}/$dataset_version}"
  STD_HDFS_PATH_FILLED="${STD_HDFS_PATH_FILLED//\{2\}/$report_date}"
  STD_HDFS_PATH_FILLED="${STD_HDFS_PATH_FILLED//\{3\}/$report_version}"

  echo "We have tmp versions to clean:"
  curl -X GET \
    --header "x-api-key: $ECS_API_KEY" \
    -d "{\"ecs_path\":\"${STD_HDFS_PATH_FILLED#s3a://}\"}" \
    $ECS_API_KK
  echo
  curl -X DELETE \
    --header "x-api-key: $ECS_API_KEY" \
    -d "{\"ecs_path\":\"${STD_HDFS_PATH_FILLED#s3a://}\"}" \
    $ECS_API_KK
  echo
  echo "Versions cleaned"
else
  echo "No std versions to clean."
fi

# At the end of the script, use the saved exit code
exit $exit_code
