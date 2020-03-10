---
layout: docs
title: Built-in Plugins
version: '2.0.0'
categories:
    - '2.0.0'
---
# Plugins

**Standardization** and **Conformance** support plugins that allow executing additional actions at certain times of the computation.

A plugin can be externally developed. In this case, in order to use the plugin a plugin jar needs to be supplied to
`spark-submit` using the `--jars` option. You can also use built-in plugins by enabling them in `application.conf`
or passing configuration information directly to `spark-submit`.

The way it works is like this. A plugin factory (a class that implements `PluginFactory`) overrides the
apply method. Standardization and Conformance will invoke this method when job starts and provides a configuration that
includes all settings from `application.conf` plus settings passed to JVM via `spark-submit`. The factory then
instantiates a plugin and returns it to the caller. If the factory throws an exception the Spark application
(Standardization or Conformance) will be stopped. If the factory returns `null` an error will be logged by the application,
but it will continue to run.

There's one type of plugins supported for now:

## Control Metrics Plugins

_Control metrics plugins_ allow execution of additional actions any time a checkpoint is created
or job status changes. In order to write such a plugin to Enceladus you need to implement the `ControlMetricsPlugin` and
`ControlMetricsPluginFactory` interfaces.

Controls metrics plugins are invoked each time a job status changes (e.g. from `running` to `succeeded`) or when a checkpoint
is reached. A `Checkpoint` is an [Atum](https://github.com/AbsaOSS/atum) concept to ensure accuracy and completeness of data.
A checkpoint is created at the end of Standardization and Conformance, and after each conformance rule
configured to create control measurements. At this point the `onCheckpoint()` callback is called with an instance of control
measurements. It is up to the plugin to decide what to do at this point. All exceptions thrown from a plugin will be
logged, but the spark application will continue to run.
