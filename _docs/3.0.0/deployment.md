---
layout: docs
title: Deployment
version: '3.0.0'
categories:
    - '3.0.0'
---

## REST API and Menas

### Prerequisits to deploying are

- Tomcat 8.5+ to deploy the war to
- `HADOOP_CONF_DIR` environment variable (for REST API). This variable should point to a folder containing Hadoop configuration files (`core-site.xml`, `hdfs-site.xml` and `yarn-site.xml`). These are used to query the HDFS for folder locations.
- MongoDB 4.0+ used as a storage
- _OPTIONAL_ [Spline 0.3.X](https://absaoss.github.io/spline/0.3.html) for viewing of the lineage from Menas. Even without Spline, Standardization and Conformance will log lineage to Mongo.

### Deploying REST API

The easiest way to deploy REST API is to copy the `rest-api-VERSION.war` to `$TOMCAT_HOME/webapps`. This will create `<tomcat IP>/rest-api-VERSION` path on your local server.

### Deploying Menas

Similarly, to deploy Menas, copy the `menas-VERSION.war` to `$TOMCAT_HOME/webapps`. This will create `<tomcat IP>/menas-VERSION` path on your local server.
