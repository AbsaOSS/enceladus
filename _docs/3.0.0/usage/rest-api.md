---
layout: docs
title: Usage - Rest API
version: '3.0.0'
categories:
    - '3.0.0'
    - usage
---

<!-- toc -->
- [General Info](#general-info)
- [Swagger documentation](#swagger-documentation)
<!-- tocstop -->


## General Info

Starting Enceladus v3.0.0, its REST API is now separated from Menas and deployed separately. Following this, the API is now
available to use for developers to integrate Enceladus programmatically.

This API provides means to manipulate entities for:
  - Datasets
  - Schemas
  - Mapping tables
  - Dataset property definitions (system-wide, needs `admin` access)
  - Runs (read only)

### Swagger documentation

The API documentation is available as a Swagger mini-site as part of the `rest_api` module deployed to webserver. 
The site can be found at `{REST_API_ROOT}/swagger-ui.html` (substitute `REST_API_ROOT` with URI prefix where the `rest_api`
is deployed, e.g. [http://localhost:8080/rest_api_war/swagger-ui.html](http://localhost:8080/rest_api_war/swagger-ui.html). 
