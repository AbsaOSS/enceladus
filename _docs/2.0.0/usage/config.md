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
[Project's readme](https://github.com/AbsaOSS/enceladus/blob/master/README.md) documents.
This page describes the configuration values stored in `spark-jobs`'s `application.conf` (or its
[template](https://github.com/AbsaOSS/enceladus/blob/master/spark-jobs/src/main/resources/application.conf.template)).
These values can be overridden using the `-D` property values as in:
```shell
spark-submit --conf "spark.driver.extraJavaOptions= -Dkey1=value1 -Dkey2=value2" ...
```

General options
-----------

|            Config path                   | Possible value(s) |        Description        |
|------------------------------------------|-------------------|---------------------------|
| `enceladus.recordId.generation.strategy` |  `uuid` (default) | `enceladus_record_id` column will be added and will contain a UUID `String` for each row. |
|                                          |  `stableHashId`   | `enceladus_record_id` column will be added and populated with an always-the-same `Int` hash (Murmur3-based, for testing). |
|                                          |  `none`           | no column will be added to the output. |
| `menas.rest.uri`                         |  string with URLs | Comma-separated list of URLs where Menas will be looked for. E.g.: `http://example.com/menas1,http://domain.com:8080/menas2` |

<!-- specific sections on Standardization & Conformance options may follow in the future -->
    
