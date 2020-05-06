---
layout: docs
title: Usage - Configuration
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
redirect_from: /docs/usage/config
---

Configuration
============

<!-- toc -->
- [Configuration](#configuration)
  - [Intro](#intro) 
  - [General options](#general-options)
<!-- tocstop -->

Intro
-----------

This page describes the usage of configuration of _Standardization_ and _Conformance_.
There are number of default options that 
[Project's readme](https://github.com/AbsaOSS/enceladus/blob/develop/README.md) documents
this page describes the configuration values stored in `spark-jobs`'s `application.conf` (or its
[template](https://github.com/AbsaOSS/enceladus/blob/develop/spark-jobs/src/main/resources/application.conf.template)).
These values can be overridden using the `-D` property values as in:
```shell
spark-submit --conf "spark.driver.extraJavaOptions= -Dkey1=value1 -Dkey2=value2" ...
```

General options
-----------

|            Config path                   |                 Description                                                                                                              |
|------------------------------------------|----------------------------------------------|
| `enceladus.recordId.generation.strategy` | If and how a record ID is generated within the `enceladus_record_id` column. Can be one of `uuid` (default - UUID-based values are generated), `stableHashId` (always-the-same Murmur3 Int hash is generated - for testing), and `none` (no column with IDs added). |
| `menas.rest.uri`                         | Comma-separated list of URLs where Menas will be looked for. |

<!-- specific sections on Standardization & Conformance options may follow in the future -->
    
