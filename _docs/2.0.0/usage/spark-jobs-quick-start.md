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

This quick start guide presumes that you have went through :

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

- `dataset_name` is the name you gave the dataset in the [Menas Quick Start]({{ docs_path }}/usage/menas-quick-start) guide Guide
- `dataset_versin` is a version of the dataset that has the correct schema and all the conformance rules you want
- `report_date` represent the date on which the data landed in the HDFS (in raw) and also the next part of the path of to the data. So if in Menas you specified `/path/on/hdfs/raw` as your raw and input `2020-12-24` as a report date then the path where standardization will look for your files will be `/path/on/hdfs/raw/2020/12/24`. For the final part we are missing report versions.
- `report_version` is the final part of the path on HDSF. With `report_date` we finished with `/path/on/hdfs/raw/2020/12/24/v<report_version>`. This is the location where standardization will look for raw data. 
- `raw-format` and its specifis. Raw format tells the standardization in which format the data is on the HDFS and what are the specifics. CSV might have header, XML has row tag, etc. Here in the example we use row-tag. For more options for different types and run parameters see our [run documentations](https://absaoss.github.io/enceladus/docs/2.0.0/usage/run) or just run `--help`


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

Here nothing new is added for the quick run. Of course there might be special options which are all documented in the [run documentations](https://absaoss.github.io/enceladus/docs/2.0.0/usage/run)

## Running with helper scripts

If your local DevOps/SysAdmin set up helper scripts for you, then it is even easier. You can omit all the spark options if sensible defaults are provided or Dynamic Resource Allocation is enabled. For more about this ask the people who set up your environment.

When scripts are properly set up, then only a few parameters need to specified.

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

For more options and arguments check the [run documentations](https://absaoss.github.io/enceladus/docs/2.0.0/usage/run)
