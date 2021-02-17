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

SETLOCAL EnableDelayedExpansion
SET RELEASE_SUBDIR=enceladus_release

IF NOT DEFINED GIT_CHECKOUT_DIR (
    ECHO GIT_CHECKOUT_DIR environment variable has to be specified. It specifies the director where release will be executed from.
    EXIT /B 1
)

IF NOT EXIST %GIT_CHECKOUT_DIR% (
    ECHO Directory specified in GIT_CHECKOUT_DIR environment variable does not exist.
    EXIT /B 1
)

CD %GIT_CHECKOUT_DIR%
IF EXIST %RELEASE_SUBDIR% (
	REN %RELEASE_SUBDIR% %RELEASE_SUBDIR%.old
	RMDIR /S /Q %RELEASE_SUBDIR%.old
)
git clone https://github.com/AbsaOSS/enceladus.git %RELEASE_SUBDIR%
CD %RELEASE_SUBDIR%
git checkout master
mvn deploy -Ppublic -Psonatype-oss-release -PgenerateComponentPreload -DskipTests
