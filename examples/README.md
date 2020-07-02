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
#### Templates which illustrate the way to write _INFO files, one with the minimally necessary fields and one with the recommended fields to add
#### Replace the <Field> tags with the appropriate data in the templates
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
* `<RecordCount>` - Number of records in Raw/Source state which should be the same for Raw and Source

[Template for bare minimum required data](examples/info_files/_INFO_file_template_minimal.json)

[Template of recommended _INFO file content](examples/info_files/_INFO_file_template_recommended.json)

## Custom Rule Implementations and usage
### Requirements
* Spark 2.2.1+ (might work with earlier, not tested)
* CSV Data Source for Apache Spark (https://github.com/databricks/spark-csv)  
Install using: `spark-shell --packages com.databricks:spark-csv_2.11:1.5.0`
### Examples
#### CustomRuleSample1
Very simple example showing how a custom rule is declared, what are its very basic parts and how it transforms some data.
#### CustomRuleSample2
Somewhat advanced example showing two possible implementations of a LPAR/RPAD custom rule. The example includes a hierarchy
of classes and usage of the new rules on hardcoded data. 
#### CustomRuleSample3
Using the previously implemented custom LPAD/RPAD rules an actual dataset is loaded from a CSV file 
(`examples/data/input/example_data.csv`), transformed and the result is shown. 
#### CustomRuleSample4
Command line driven example. Data are loaded from the file specified (supporting multiple formats), transformed and 
saved to a csv file on a specified path.  
NB! As the transformation rules are hardcoded the input file have to have a column named **text_column**

For command-line arguments description run:  
`spark-submit --class za.co.absa.enceladus.examples.CustomRuleSample4 --master local enceladus-examples.jar --help`

Example arguments:  
`spark-submit --class za.co.absa.enceladus.examples.CustomRuleSample4 --master local enceladus-examples.jar --input-file="data/input/example_data.csv" --out-path="data/output" --header=true`

### Best Practices
* It's a good idea when creating custom rules the cover their logic with unit tests as well. 
See the tests in `test/scala/class/za/co/absa/enceladus/examples/interpreter/rules/custom/`
