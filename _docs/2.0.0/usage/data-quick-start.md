---
layout: docs
title: Usage - Data & Data Quality Quick Start
version: '2.0.0'
categories:
    - '2.0.0'
    - 'usage'
---
{% capture docs_path %}{{ site.baseurl }}/docs/{{ page.version }}{% endcapture %}

## Prerequisites

This quick start guide presumes that you have went through :

- [Menas Quick Start]({{ docs_path }}/usage/menas-quick-start) guide

## Data Quality

Data quality is all about 2 things:

- Spline
- _INFO files

## Spline

but as Spline is under heavy development, we will postpone extensive documentation about it. Enceladus currently runs with version 0.3.X and it works fine out of the box. Spline 0.3.X just needs to be deployed next to Menas.

Data for Spline are recorded even if the Spline UI is not up and running. This means they can be viewed later without the need to care bout it now.

More about spline at [Spline](https://github.com/AbsaOSS/spline) Github.

## _INFO files

`_INFO` files our our way of tracking where the data came from and how much is there. It checks mainly that we did not loose any data on the way to standardization and conformance. All this made possibleby [Atum](https://github.com/AbsaOSS/atum). `_INFO` files should be placed withing the source directory together with the raw data.

More info about `_INFO` files [here](https://absaoss.github.io/enceladus/docs/2.0.0/usage/info-file).

Examples of `_INFO` files [here](https://github.com/AbsaOSS/enceladus/tree/develop/examples).

[**Next Spark Jobs Quick Sart**]({{ site.baseurl }}/docs/{{ page.version }}/usage/spark-jobs-quick-start)
