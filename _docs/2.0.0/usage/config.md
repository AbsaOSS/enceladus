---
layout: docs
title: Usage - Configuration
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
---
## Table Of Contents
<!-- toc -->
- [Table Of Contents](#table-of-contents)
- [Intro](#intro)
- [General options](#general-options)
- [Selected plugin options](#selected-plugin-options)
<!-- tocstop -->

## Intro

This page describes the usage of configuration of _Standardization_ and _Conformance_.
There are number of default options that [Project's readme][readme] documents.
This page describes the configuration values stored in `spark-jobs`'s `reference.conf` ([link][spark-app-conf]) or `application.conf` provided by the user.
These values can be overridden using the `-D` property values as in:

```shell
spark-submit --conf "spark.driver.extraJavaOptions= -Dkey1=value1 -Dkey2=value2" ...
```

## General options

{% include config_options.html file="configuration_2_0_0" %}

<!-- specific sections on Standardization & Conformance options may follow in the future -->

Note that

<sup>*</sup> <a id="note1"></a> `conformance.autoclean.standardized.hdfs.folder` when set to true and the job is writing to S3, there could be a leftover empty file like `conformance-output_$folder$` after the autoclean.
This, however, will not negatively impact the functionality of other jobs even when using the same path and is due to the EMR committer.


## Selected plugin options:
{% include config_options.html file="selected-plugins-configuration_2_0_0" %}


[readme]: https://github.com/AbsaOSS/enceladus/blob/master/README.md
[spark-app-conf]: https://github.com/AbsaOSS/enceladus/blob/master/spark-jobs/src/main/resources/reference.conf
