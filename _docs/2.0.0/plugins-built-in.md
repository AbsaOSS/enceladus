---
layout: docs
title: Built-in Plugins
version: '2.0.0'
categories:
    - '2.0.0'
redirect_from: /docs/plugins-built-in
---
<!-- toc -->
- [What are built-in plugins](#what-are-built-in-plugins)
- [Existing built-in plugins](#existing-built-in-plugins)
  - [KafkaInfoPlugin](#kafkainfoplugin)
<!-- tocstop -->

What are built-in plugins
-------------------------

Built-in plugins provide some additional but relatively elementary functionality. And also serve as an example how plugins 
are written. Unlike externally created plugins they are automatically included in the `SparkJobs.jar` file and therefore 
don't need to be included using the `--jars` option.

Existing built-in plugins
-------------------------

### KafkaInfoPlugin

The purpose of this plugin is to send control measurements to a Kafka topic each time a checkpoint is reached or job
status is changed. This can help to monitor production issues and react to errors as quickly as possible.
Control measurements are sent in `Avro` format and the schema is automatically registered in a schema registry.

This plugin is a built-in one. In order to enable it, you need to provide the following configuration settings in
`application.conf`:
```
standardization.plugin.control.metrics.1=za.co.absa.enceladus.plugins.builtin.controlinfo.mq.kafka.KafkaInfoPlugin
conformance.plugin.control.metrics.1=za.co.absa.enceladus.plugins.builtin.controlinfo.mq.kafka.KafkaInfoPlugin
kafka.schema.registry.url:"http://127.0.0.1:8081"
kafka.bootstrap.servers="127.0.0.1:9092"
kafka.info.metrics.client.id="controlInfo"
kafka.info.metrics.topic.name="control.info"
# Optional security settings
#kafka.security.protocol="SASL_SSL"
#kafka.sasl.mechanism="GSSAPI"
```

The plugin class name is specified for Standardization and Conformance separately since some plugins need to run only
during execution of one of these jobs. Plugin class name keys have numeric suffixes (`.1` in this example). The numeric
suffix specifies the order at which plugins are invoked. It should always start with `1` and be incremented by 1 without
gaps.
