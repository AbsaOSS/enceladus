---
layout: docs
title: Usage - Menas API
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
---
## Table Of Contents
<!-- toc -->
- [Table Of Contents](#table-of-contents)
- [Export](#export)
- [Import](#import)
<!-- tocstop -->

## Export

`GET /menas/api/{entity}/exportItem/{name}/{version}`

to export a particular item
 
`GET /menas/api/{entity}/exportItem/{name}`

to export the latest item

**Entities:**
* dataset
* schema
* mapping table
 
## Import

`POST /menas/api/{entity}/importItem`

to import an item

**Entities:**
* dataset
* schema
* mapping table
 
