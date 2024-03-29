                    Copyright 2021 ABSA Group Limited
                  
      Licensed under the Apache License, Version 2.0 (the "License");
      you may not use this file except in compliance with the License.
                You may obtain a copy of the License at
               http://www.apache.org/licenses/LICENSE-2.0
            
     Unless required by applicable law or agreed to in writing, software
       distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      See the License for the specific language governing permissions and
                      limitations under the License.

# Examples of Enceladus E2E data

Main purpose of the data presented here is for use in Enceldus e2e automated tests,
but it also nicely shows examples of data and combinations possible.

There are 3 folders

- `data` - data for hdfs and jsons ready to be imported into Enceladus
- `setup_scripts` - should help you with importing data into Enceladus and copying onto HDFS
- `test_jsons` - jsons ready to be executed using [Hermes](https://github.com/AbsaOSS/hermes). 
For executing these tests, it should be changed `recordId.generation.strategy` to `stableHashId`. 

## Setup Scripts

### Import into Enceladus

**Tools used**: cURL

This script will populate Enceladus with datasets, schemas and mapping tables.
To run scripts with user prompts, run:

```shell
./setup_scripts/import_all.sh
```

Ths will start the script and ask you for a username, password and Enceladus REST API URL.

To run without the prompts use:
```shell
./setup_scripts/import_all.sh with <username> <password> <rest_api url>
```
or 
```shell
./setup_scripts/import_all.sh with defaults
```

The parameters are user name, password and REST API URL.
Defaults are username: `user` and password: `changeme`. 

### Copy to HDFS

**Tools used**: hdfs dfs

This script will upload data to your HDFS, so you would be able to run the datasets in Enceladus. 
To run this script with user prompts, run:

```shell
./setup_scripts/copy_to_hdfs.sh
```

This will start the script and ask you for the root folder of to use. 
This is a folder into which it will copy the folders from `/data/hdfs`.

If the folder does not exist, you will be asked if you want to create it.

To run without user prompts use

```shell
./setup_scripts/copy_to_hdfs.sh with /path/on/hdfs [y/n]
```
First parameter is the custom set root folder in HDFS. 
The second parameter answers if to create this folder (`y`) in case it doesn't exist or not (`n`). 
Second parameter is optional.

If you run it with defaults, then it will copy everything into the root of the hdfs (`/`) 
```shell
./setup_scripts/copy_to_hdfs.sh with defaults
```
