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

# This is a wrapper script for Enceladus run scripts.
# It is used to resolve the paths to the JCEKS files and
# to clean up the versions in Dell ECS after the run.
# To use it just prepend your Enceladus run script by this script
# Example:
# ./s3a_wrapper.sh run_standardization_conformance.sh \
#     --dataset-name "MyDataset"
#     --dataset-version 1
#     --report-date "2018-01-01"
#     --report-version 1
#
# If the dataset is published to S3A, the script will find the JCEKS file on its own.
# If the dataset is not published to S3A, the script will just pass the arguments to the enceladus run script.
# If the dataset is published to S3A and you pass --jceks-path argument, the script will use the provided JCEKS conf.
# Format of the --jceks-path argument is:
#   "spark.hadoop.fs.s3a.bucket.$BUCKET_NAME.security.credential.provider.path=jceks:$JCEKS_PATH"
#
# Requirements:
#   - jq
#   - curl
#   - klist

set -e

# Run klist to check for a current Kerberos ticket
if klist -s; then
    echo "Kerberos ticket found."
else
    error_exit "No Kerberos ticket found or ticket is expired. Please run kinit."
fi

# Source environment variables
source "$(dirname "$0")/enceladus_env.sh"

# The first argument is the name of the original script
original_script="$(dirname "$0")/$(basename "$1")"
# Shift the first argument so we can process the rest
shift

hdfsPublishPath=""
hdfsPath=""
jceks_flag=""
std_path="$STD_HDFS_PATH"

# Function to print error message and exit
function error_exit() {
  echo "Error: $1" >&2
  exit 1
}

# Function to get dataset information
function get_dataset_info() {
  local response=""

  response=$(curl --negotiate -s -u : "$MENAS_API/dataset/$dataset_name/$dataset_version")
  [[ $? -ne 0 ]] && error_exit "Could not load dataset info - $dataset_name v $dataset_version from Menas at $MENAS_API"

  hdfsPublishPath=$(echo "$response" | jq -r '.hdfsPublishPath')
  hdfsPath=$(echo "$response" | jq -r '.hdfsPath')
  [[ $hdfsPublishPath == "null" || $hdfsPath == "null" ]] && error_exit "Could not find the required paths in the response."
  return 0
}

# Function to handle JCEKS and set jceks_flag if need be
function handle_jceks_path() {
  if [[ $hdfsPublishPath =~ ^s3a://.* ]]; then
    echo "hdfsPublishPath starts with s3a://. Using JCEKS file."
    if [[ -z $jceks_path ]]; then
      readwrite_jceks=$(curl -s -X GET -d "{\"ecs_path\":\"$hdfsPublishPath\"}" "$ECS_API_BUCKET" | jq -r '.readwrite_jceks')
      [[ -z $readwrite_jceks ]] && error_exit "Could not find readwrite_jceks in the response."
      bucket_name=$(echo "$hdfsPublishPath" | cut -d'/' -f3)
      jceks_flag="--jceks-path \"spark.hadoop.fs.s3a.bucket.$bucket_name.security.credential.provider.path=jceks:$readwrite_jceks\""
    else
      echo "--jceks-path argument is set by user"
      jceks_flag="--jceks-path $jceks_path"
    fi
  fi
  return 0
}

# Function to clean up versions
function cleanup_versions() {
  local path=$1
  local api=$2
  echo "Cleaning versions for $path"
  curl -s -X GET --header "x-api-key: $ECS_API_KEY" -d "{\"ecs_path\":\"${path#s3a://}\"}" "$api"
  echo
  curl -s -X DELETE --header "x-api-key: $ECS_API_KEY" -d "{\"ecs_path\":\"${path#s3a://}\"}" "$api"
  echo
  echo "Versions cleaned"
  return 0
}

# Function to get evaluated tmp path for standardization
# Replace placeholders if present
function replace_std_path_placeholders() {
  std_path=$(echo "$std_path" | sed "s/{0}/$dataset_name/")
  std_path=$(echo "$std_path" | sed "s/{1}/$dataset_version/")
  std_path=$(echo "$std_path" | sed "s/{2}/$report_date/")
  std_path=$(echo "$std_path" | sed "s/{3}/$report_version/")

  return 0
}


# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --menas-auth-keytab) keytab="$2"; shift 2 ;;
    --dataset-name) dataset_name="$2"; shift 2 ;;
    --dataset-version) dataset_version="$2"; shift 2 ;;
    --report-date) report_date="$2"; shift 2 ;;
    --report-version) report_version="$2"; shift 2 ;;
    --jceks-path) jceks_path="$2"; shift 2 ;;
    *)
      if [ -z "$1" ]; then
        # If the argument is an empty string, add two quotes to represent it
        other_args+=('""')
      elif [ "$1" == "''" ]; then
        # If the argument is ''
        other_args+=("\'\'")
      elif [ "$1" == "|" ]; then
        # If the argument is a pipe character
        other_args+=("'|'")
      elif [[ "$1" =~ ^[[:space:]]+$ ]]; then
        # If the argument is one or more space characters
        other_args+=("'$1'")
      else
        other_args+=("$1")
      fi
      shift ;;
  esac
done

[[ ! -f "$original_script" ]] && error_exit "The script '$original_script' does not exist in the current directory."

# Main script execution
get_dataset_info
handle_jceks_path

# Run the original script
echo "$original_script" "${other_args[@]}" \
  --menas-auth-keytab "$keytab" \
  --dataset-name "$dataset_name" \
  --dataset-version "$dataset_version" \
  --report-date "$report_date" \
  --report-version "$report_version" \
  "$jceks_flag" | bash

exit_code=$?

# Clean up versions if necessary
[[ $hdfsPublishPath == s3a://* ]] && cleanup_versions "$hdfsPublishPath/enceladus_info_date=$report_date" "$ECS_API_KK"
[[ $std_path == s3a://* ]] && replace_std_path_placeholders && cleanup_versions "$std_path"

exit $exit_code
