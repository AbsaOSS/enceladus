---
layout: docs
title: Usage - Spark Job Quick Start
version: '2.0.0'
categories:
    - '2.0.0'
    - 'usage'
redirect_from: /docs/usage/spark-jobs-quick-start
---
{% capture docs_path %}{{ site.baseurl }}/docs/{{ page.version }}{% endcapture %}

## Prerequsites

This quick start guide presumes that you have gone through :

- [Menas Quick Start]({{ docs_path }}/usage/menas-quick-start) guide
- [Data & Data Quality Quick Start]({{ docs_path }}/usage/data-quick-start) guide

## Running with spark-submit

### Running Standardization

```shell
<spark home>/spark-submit \
--num-executors 2 \
--executor-memory 2G \
--master yarn \
--deploy-mode cluster \
--driver-cores 2 \
--driver-memory 2G \
--class za.co.absa.enceladus.standardization.StandardizationJob \
spark-jobs_<build_version>.jar \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <report_date> \
--report-version <report_version> \
--raw-format <data_format> \
--row-tag <tag>
```

where:

- `dataset_name` is the name given the dataset per [Menas Quick Start]({{ docs_path }}/usage/menas-quick-start) guide Guide
- `dataset_version` is a version of the dataset to use, which should have the correct schema and all the desired conformance rules 
- `report_date` represents the date on which the data landed in the HDFS (in raw) and also the suffix part of the data output path. So if in Menas raw was specified as `/path/on/hdfs/raw` and the input parameter `report_date` as `2020-12-24` then the path where standardization will look for input files will be `/path/on/hdfs/raw/2020/12/24`. For the final part we are missing the report versions.
- `report_version` is the final part of the path on HDSF. With `report_date` we finished with `/path/on/hdfs/raw/2020/12/24/v<report_version>`. This is the location where standardization will look for raw data.
- `raw-format` and its specifics. Raw format tells the standardization which format the data is in on the HDFS and what are its specifics. CSV might have a header, XML has a row-tag, etc. Here in the example, we use the `row-tag`. For more options for different types and run parameters see our [run documentation](https://absaoss.github.io/enceladus/docs/2.0.0/usage/run) or just run `--help`


### Running Conformance

```shell
<spark home>/spark-submit \
--num-executors 2 \
--executor-memory 2G \
--master yarn \
--deploy-mode cluster \
--driver-cores 2 \
--driver-memory 2G \
--class za.co.absa.enceladus.conformance.DynamicConformanceJob \
spark-jobs_<build_version>.jar \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <report_date> \
--report-version <data_run_version>
```

### Running Standardization and Conformance together

```shell
<spark home>/spark-submit \
--num-executors 2 \
--executor-memory 2G \
--master yarn \
--deploy-mode cluster \
--driver-cores 2 \
--driver-memory 2G \
--class za.co.absa.enceladus.standardization_conformance.StandardizationAndConformanceJob \
spark-jobs_<build_version>.jar \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <report_date> \
--report-version <report_version> \
--raw-format <data_format> \
--row-tag <tag>
```

Here, nothing new is added for the quick run. Of course, there might be special options which are all documented in the [run documentation](https://absaoss.github.io/enceladus/docs/2.0.0/usage/run)

## Running with helper scripts

If your local DevOps/SysAdmin set up helper scripts for you, then it is even easier. You can omit all the spark options if sensible defaults are provided or Dynamic Resource Allocation is enabled. For more about this ask the people who set up your environment.

Steps to configure the scripts are as follows (_Linux_/_Windows_):
* Copy all the scripts in `scripts/bash`/`scripts/c,d` directory to a location in your environment.
* Copy `enceladus_env.template.sh`/`enceladus_env.template.cmd` to `enceladus_env.sh`/`enceladus_env.cmd`.
* Change `enceladus_env.sh`/`enceladus_env.cmd` according to your environment settings.
* Use `run_standardization.sh`/`run_standardization.cmd` and `run_conformance.sh`/`run_conformance.cmd` or `run_standardization_conformance.sh`/`run_standardization_conformance.cmd` scripts instead of directly invoking `spark-submit` to run your jobs.

Similar scripts exist for _Windows_ in directory `scripts/cmd`.

When scripts are properly set up, then only a few parameters need to be specified.

### Linux

The basic command to run Standardization becomes:

```shell
<path to scripts>/run_standardization.sh \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <report_date> \
--report-version <data_run_version> \
--raw-format <data_format> \
--row-tag <tag>
```

The basic command to run Conformance becomes:

```shell
<path to scripts>/run_conformance.sh \
--deploy-mode <client/cluster> \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <report_date> \
--report-version <data_run_version>
```

The basic command to run Standardization and Conformance together becomes:

```shell
<path to scripts>/run_standardization_conformance.sh \
--menas-auth-keytab <path_to_keytab_file> \
--dataset-name <dataset_name> \
--dataset-version <dataset_version> \
--report-date <report_date> \
--report-version <data_run_version> \
--raw-format <data_format> \
--row-tag <tag>
```

### Windows

The basic command to run Standardization becomes:

```cmd
<path to scripts>/run_standardization.cmd ^
--menas-auth-keytab <path_to_keytab_file> ^
--dataset-name <dataset_name> ^
--dataset-version <dataset_version> ^
--report-date <report_date> ^
--report-version <data_run_version> ^
--raw-format <data_format> ^
--row-tag <tag>
```

The basic command to run Conformance becomes:

```cmd
<path to scripts>/run_conformance.cmd ^
--deploy-mode <client/cluster> ^
--menas-auth-keytab <path_to_keytab_file> ^
--dataset-name <dataset_name> ^
--dataset-version <dataset_version> ^
--report-date <report_date> ^
--report-version <data_run_version>
```

The basic command to run Standardization and Conformance together becomes:

```cmd
<path to scripts>/run_standardization_conformance.cmd ^
--menas-auth-keytab <path_to_keytab_file> ^
--dataset-name <dataset_name> ^
--dataset-version <dataset_version> ^
--report-date <report_date> ^
--report-version <data_run_version> ^
--raw-format <data_format> ^
--row-tag <tag>
```


For more options and arguments check the [run documentation](https://absaoss.github.io/enceladus/docs/2.0.0/usage/run)
