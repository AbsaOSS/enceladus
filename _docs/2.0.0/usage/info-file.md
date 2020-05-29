---
layout: docs
title: Usage - _INFO file 
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
redirect_from: /docs/usage/info-file
---
## Table Of Content

<!-- toc -->
- [Table Of Content](#table-of-content)
- [Description](#description)
- [Validation](#validation)
- [Additional Information](#additional-information)
<!-- tocstop -->

## Description

File named `\_INFO` placed within the source directory together with the raw data is a JSON file tracking control measures 
via [Atum][atum]. Example how the file should contain can be found
[in the code][info-file].

## Validation

The _\_INFO file_ verification consists of checking that it has an array field of name `checkpoints`. The array has to
have at least two objects one named (field `name`) _"Raw"_ and one _"Source"_. Each of them has to have an array field
`controls`. This array has to contain a control of type count (`"controlType": "controlType.Count"`) with control value
(field `controlValue`) containing a positive integer.

E.g.

```json
{
  ...
  "checkpoints": [
    {
      "name": "Source",
      "processStartTime": "??? (timestamp)",
      "processEndTime": "??? (timestamp)",
      "workflowName": "Source",
      "order": "??? (positive integer)",
      "controls": [
        {
          "controlName": "recordCount",
          "controlType": "controlType.Count",
          "controlCol": "???",
          "controlValue": "??? (positive integer)"
        }
      ]
    },
    {
      "name": "Raw",
      "processStartTime": "??? (timestamp)",
      "processEndTime": "??? (timestamp)",
      "workflowName": "Raw",
      "order": "???",
      "controls": [
        {
          "controlName": "recordCount",
          "controlType": "controlType.Count",
          "controlCol": "???",
          "controlValue": "??? (positive integer)"
        }
      ]
    }
  ]
}
```

For a fully expanded example go [here][info-file].

## Additional Information 

Addional information regarding the processing of information is added into the \_INFO file during **Standardization** and **Conformance**.

|           Metadata-Key      | Description |
|-----------------------------|-----------------|
| `conform_driver_memory`     | The amount of memory used to run Conformance |
| `conform_enceladus_version` | Which version of Enceladus was used to run Conformance |
| `conform_errors_count`      | Number of errors after conformance |
| `conform_executor_memory`   | Number of executors running conformance |
| `conform_executors_num`     | How many executors used for conformance |
| `conform_input_data_size`   | The size of the input data (without metadata) to the Conformance.Usually it is the same as the size of standardized data since Conformance is ran after Standardization |
| `conform_output_data_size`  | The size of conformed/published data (without metadata such as lineage or _INFO file) |
| `conform_output_dir_size`   | The size of the published directory including metadata |
| `conform_records_failed`    | Number of records that has at least one error after Conformance |
| `conform_size_ratio`        | Size of the conformed/published folder in relation to a standardized folder |
| `conform_spark_master`      | Spark master of the Conformance job (usually yarn) |
| `conform_username`          | User account under which Conformance was performed |
| `csv_delimiter`             | dependant on the input file eg. csv |
| `raw_format`                | Format of raw data, eg. `csv`, `json`, `xml`, `cobol` |
| `source_record_count`       | The number of records in the dataset when it was exported from the source syste |
| `std_application_id`        | Spark Application unique id of the Standardization Job |
| `std_errors_count`          | Number of errors after standardization |
| `std_executor_memory`       | Memory requested per executor for Standardization |
| `std_executors_num`         | How many executors used for Standardization |
| `std_input_dir_size`        | The size of the raw folder |
| `std_output_data_size`      | Size of the output data after standardization |
| `std_output_dir_size`       | The size of the standardized folder |
| `std_records_failed`        | Number of records that has at least one error after standardization |
| `std_records_succeeded`     | Number of records that has no errors after standardization |
| `std_spark_master`          | Spark master of the Standardization job (usually yarn) |
| `std_username`              | User account under which Standardization was performed |
| `std_yarn_deploy_mode`      | Yarn deployment mode used (client or cluster) |

[atum]: https://github.com/AbsaOSS/atum
[info-file]: https://github.com/AbsaOSS/enceladus/blob/master/examples/data/input/_INFO
