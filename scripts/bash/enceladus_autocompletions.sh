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

# An autocompletion script for Enceladus's standardization and conformance
# Expects
#   "$PATH_TO_SCRIPTS/run_standardization.sh - to be path to executable standardization script
#   "$PATH_TO_SCRIPTS/run_conformance.sh - to be path to executable conformance script
_get_basic_options(){
  local eSparkOpts=$(echo $spark_opts | tr " " "\n")
  local eCompWords=$( IFS=$'\n'; echo "${COMP_WORDS[*]}" )
  local temp_opts=$(comm -23 <(echo -e "$eSparkOpts" | sort) <(echo -e "$eCompWords\n" | sort))
  COMPREPLY=( $(compgen -W "${temp_opts}" -- ${cur}) )
  return 0
}

_generic_job()
{
  local cur prev spark_opts conf_opts

  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"
  spark_opts="--num-executors --executor-memory --deploy-mode --master --driver-cores \
              --driver-memory --class --conf --rest-api-auth-keytab --dataset-name --dataset-version --report-date \
              --report-version ${specific_conf_opts} --performance-file --folder-prefix --help"

  if [[ "$prev" = "--rest-api-auth-keytab" ]] || [[ "$prev" = "--performance-file" ]] ; then
    _filedir
    return 0
  elif [[ "$prev" = "--deploy-mode" ]]; then
    COMPREPLY=( $(compgen -W "client cluster" -- ${cur}) )
    return 0
  else
    _get_basic_options
  fi
}

_conformance()
{
  local specific_conf_opts
  COMPREPLY=()
  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"
  specific_conf_opts="--debug-set-publish-path --experimental-mapping-rule"

  _generic_job
}

_standardization()
{
  local specific_conf_opts
  COMPREPLY=()
  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"
  specific_conf_opts="--raw-format --row-tag --delimiter --header --trimValues --debug-set-raw-path"

  _generic_job
}

alias conformance="$PATH_TO_SCRIPTS/run_conformance.sh"
alias standardization="$PATH_TO_SCRIPTS/run_standardization.sh"

complete -F _standardization standardization
complete -F _conformance conformance