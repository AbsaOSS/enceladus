---
layout: docs
title: Usage - Menas Configuration
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
<!-- tocstop -->

## Intro

This page describes the usage of the configuration of _Menas_.
This page describes the configuration values stored in `menas`'s `application.properties` (or its
[template][app-props-template]).

## General options

{% include config_options.html file="menas-configuration_2_0_0" %}

Note, that
  - `menas.schemaRegistry.baseUrl` may not be present (in that case, the option to load schema from a schema registry
  by subject name will disappear in the Menas UI)
  - specifying `javax.net.ssl.{trustStore|keyStore}` (and the passwords) is usually both necessary to successfully load
  a schema file from a secure schema registry, but this setting will be used by the by-URL loading as well 
  (if supported by webserver reached)
  

[readme]: https://github.com/AbsaOSS/enceladus/blob/master/README.md
[app-props-template]: https://github.com/AbsaOSS/enceladus/blob/master/menas/src/main/resources/application.properties.template
