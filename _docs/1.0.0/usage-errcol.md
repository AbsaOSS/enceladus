---
layout: docs
title: Usage
version: '1.0.0'
categories:
    - '1.0.0'
---

Error Column
============

<!-- toc -->
- [Description](#description)
- [Error Types](#error-types)
  - [stdCastError](#stdcasterror)
  - [stdNullErr](#stdnullerr)
  - [confMappingErr](#confmappingerr)
  - [confCastErr](#confcasterr)
  - [confNegErr](#confnegerr)
  - [confLitErr](#confliterr)
- [Notes](#notes)
<!-- tocstop -->

Description
-----------

`errCol` is a special, automatically created, composite column, that contains descriptions of all the issues encountered
during **Standardization** and **Conformance** of the particular row.

It's an array, where each member represents one error that happened on the particular row during its processing.
The array member is a structured information itself:

* `errType` - string representation of the type of the error
* `errCode` - code representation of the type of the error in the form of _E#####_, where # is a digit (e.g. E00001)
* `errMsg` - description of the error
* `errCol` - name of the column, in which the error happened [\*](#notes-star)
* `rawValues` - the input values for the error
* `mappings` - ???


Error Types
-----------

### `stdCastError`

When column value fails to standardize to the expected type

### `stdNullErr`

When column value is `null` in non-nullable column during standardization 

### `confMappingErr`

Mapping of the value failed during conformance

### `confCastErr`

When casting fails during conformance

### `confNegErr`

When negation of numeric type with minimum value overflows during conformance

### `confLitErr`

When during Conformance special column value has changed

Notes
-----

<a name="#notes-star" />\* When **Standardization** of a value fails and the column has a `sourcecolumn` *metadata* property defined, the
`sourcecolumn` value, the actual source of the data, will be mentioned in the error. Not the output column name. 
