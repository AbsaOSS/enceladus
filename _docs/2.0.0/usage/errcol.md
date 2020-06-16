---
layout: docs
title: Usage - Error Column
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
---
## Table Of Content
<!-- toc -->
- [Table Of Content](#table-of-content)
- [Description](#description)
- [Error Types](#error-types)
- [Notes](#notes)
<!-- tocstop -->

## Description

`errCol` is a special, automatically created, composite column, that contains descriptions of all the issues encountered
during **Standardization** and **Conformance** of the particular row.

It's an array, where each member represents one error that happened on the particular row during its processing.
The array element is structured as follows:

- `errType` - string representation of the type of the error
- `errCode` - code representation of the type of the error in the form of _E#####_, where # is a digit (e.g. E00001)
- `errMsg` - description of the error
- `errCol` - name of the column, in which the error occurred [*](#notes-star)
- `rawValues` - the input values for the error
- `mappings` - ???

## Error Types

| Error type      | Description |
|-----------------|-------------|
| `stdCastError`  | Column value failed to standardize to the expected type |
| `stdNullErr`    | Column value was `null` in non-nullable column during standardization |
| `confMappingErr`| Mapping of the value failed during conformance |
| `confCastErr`   | Casting failed during conformance |
| `confNegErr`    | Negation of numeric type with minimum value overflowed during conformance |
| `confLitErr`    | During Conformance special column value has changed |

## Notes

<a name="#notes-star" />\* When **Standardization** of a value fails and the column has a `sourcecolumn` *metadata* property defined, the
`sourcecolumn` value, the actual source of the data, will be mentioned in the error. Not the output column name.
