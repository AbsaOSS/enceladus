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

SETLOCAL EnableDelayedExpansion
SET PATH=%~dp0;%PATH%

CALL enceladus_env.cmd

SET CLASS=za.co.absa.enceladus.conformance.DynamicConformanceJob

SET DEFAULT_DRIVER_MEMORY=%CONF_DEFAULT_DRIVER_MEMORY%
SET DEFAULT_DRIVER_CORES=%CONF_DEFAULT_DRIVER_CORES%
SET DEFAULT_EXECUTOR_MEMORY=%CONF_DEFAULT_EXECUTOR_MEMORY%
SET DEFAULT_EXECUTOR_CORES=%CONF_DEFAULT_EXECUTOR_CORES%
SET DEFAULT_NUM_EXECUTORS=%CONF_DEFAULT_NUM_EXECUTORS%

SET DEFAULT_DRA_ENABLED=%CONF_DEFAULT_DRA_ENABLED%

SET DEFAULT_DRA_MIN_EXECUTORS=%CONF_DEFAULT_DRA_MIN_EXECUTORS%
SET DEFAULT_DRA_MAX_EXECUTORS=%CONF_DEFAULT_DRA_MAX_EXECUTORS%
SET DEFAULT_DRA_ALLOCATION_RATIO=%CONF_DEFAULT_DRA_ALLOCATION_RATIO%
SET DEFAULT_ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE=%CONF_DEFAULT_ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE%

CALL run_enceladus.cmd %*
