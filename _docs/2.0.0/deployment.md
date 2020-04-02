---
layout: docs
title: Deployment
version: '2.0.0'
categories:
    - '2.0.0'
---

## Menas

### Prerequisits to deploying Menas are

- Tomcat 8.5+ to deploy the war to
- HADOOP_CONF_DIR environment variable. This variable should point to a folder containing hoodp configuration files (core-site.xml, hdfs-site.xml and yarn-site.xml). These are used to querry HDFS for folder locations
- MongoDB 4.0+ used as a storage

### Deploying Menas

Easiest way to deploy menas is to copy the `menas-VERSION.war` to `$TOMCAT_HOME/webapps`. This will create `<tomcat IP>/menas-VERSION` path on your local server.
