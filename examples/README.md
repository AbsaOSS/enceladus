                    Copyright 2018 ABSA Group Limited
                  
      Licensed under the Apache License, Version 2.0 (the "License");
      you may not use this file except in compliance with the License.
                You may obtain a copy of the License at
               http://www.apache.org/licenses/LICENSE-2.0
            
     Unless required by applicable law or agreed to in writing, software
       distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      See the License for the specific language governing permissions and
                      limitations under the License.

# Examples of Enceladus usage

## Templates for _INFO files

 Templates which illustrate the way to write _INFO files, one with the minimally necessary fields and one with the recommended fields to add.
    
 More info regarding the _INFO file validation can be found in the [INFO file Documentation](https://absaoss.github.io/enceladus/docs/usage/info-file).
 
 Replace the <Field> tags with the appropriate data in the templates.
 
### Fields changed once per dataset

* `<SourceApplication>` - name of the application
* `<Country>` - country the file belongs to
* `<SourceType>` - source type
* `<ColumnName>` - ideally the key column name, but can be any column

### Fields (possibly) changed for every run

* `<Filename>` - source file name
* `<Version>` - version of the source file (integer)
* `<Date>` - date of the input processing

###Optionally:
* `<ProcessStartTime>` - Start of the processing; String timestamp in format ‘dd-MM-yyyy HH:mm:ss’ or ‘dd-MM-yyyy HH:mm:ss ZZZ’(doesn't have to be the same for Raw and Source)
* `<ProcessEndTime>` - End of the processing; String timestamp in format ‘dd-MM-yyyy HH:mm:ss’ or ‘dd-MM-yyyy HH:mm:ss ZZZ’(doesn't have to be the same for Raw and Source)
* `<RecordCount>` - Number of records, should be the same for Raw and Source. In case of an unknown value, provide `""`(empty string) and ensure [`control.info.validation` configuration](https://absaoss.github.io/enceladus/docs/usage/config) is set to `Warning` or `None`

[Template for bare minimum required data](info_files/_INFO_file_template_minimal.json)

[Template of recommended _INFO file content](info_files/_INFO_file_template_recommended.json)

## REST API

List of end-points of the _**rest-api**_ module in a form of example calls for _Postman_ application. See the local 
[README.md](rest_api/README.md) for details.