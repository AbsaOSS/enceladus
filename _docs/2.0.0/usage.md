---
layout: docs
title: Usage
version: '2.0.0'
categories:
    - '2.0.0'
redirect_from: /docs/usage
---
{% capture docs_path %}{{ site.baseurl }}/docs/{{ page.version }}{% endcapture %}

## Table Of Contents

<!-- toc -->
- [Table Of Contents](#table-of-contents)
- [Intro](#intro)
- [Quick Start](#quick-start)
- [Spark Jobs](#spark-jobs)
- [General](#general)
- [Details](#details)
<!-- tocstop -->

## Intro

This part of the documentations will show you how to use Enceladus, Menas as its UI and how to run its spark-jobs. This part expects you to have Menas already deployed and running and `spark-jobs.jar` ready at hand. If not, please look at previous parts of [build process][build] and [deployment][deploy]

## Quick Start

- [Menas Quick Start]({{ docs_path }}/usage/menas-quick-start)
- [Data & Data Quality Quick Start]({{ docs_path }}/usage/data-quick-start)
- [Spark Jobs Quick Start]({{ docs_path }}/usage/spark-jobs-quick-start)

## Spark Jobs

## General

- [Configuration]({{ docs_path }}/usage/config)
- [How to run]({{ docs_path }}/usage/run)
- [\_INFO file]({{ docs_path }}/usage/info-file)
- [Schema]({{ docs_path }}/usage/schema)

## Details

- [Error columns]({{ docs_path }}/usage/errcol)

[build]: {{ docs_path }}/build-process
[deploy]: {{ docs_path }}/deployment
