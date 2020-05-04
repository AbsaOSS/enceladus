---
layout: docs
title: Components
version: '2.0.0'
categories:
    - '2.0.0'
redirect_from: /docs/components
---

### Menas

Menas is a UI component of the Enceladus project. It is used to define datasets and schemas representing your data. Using dataset definition you define where the data is, where should it land if any conformance rules should be applied. Schema defines how does the data will look (column names, types) after standardization.

[More...]({{ site.baseurl }}/docs/{{ page.version }}/components/menas)

### SparkJobs

Enceladus consists of two spark jobs. One is Standardization, for alignation of data types and format, and the second one is Conformance, which then applies conformance rules onto the data.

#### Standardization

Standardization is used to transform almost any data format into a standardized, strongly typed parquet format, so the data can be used/view using unified tools.

#### Conformance

Conformance is used to apply conformance rules (mapping, negation, casting, etc.) onto the data. Conformance rules are additional tranformations of the data.

### Plugins

[More...]({{ site.baseurl }}/docs/{{ page.version }}/plugins)

### Built-in Plugins

[More...]({{ site.baseurl }}/docs/{{ page.version }}/plugins-built-in)
