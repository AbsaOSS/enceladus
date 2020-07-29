---
layout: docs
title: Usage - Standardization Input Formats
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
---

Currently Standardization supports these formats of input files

- [Cobol](#cobol) (see [Cobrix GitHub](https://github.com/AbsaOSS/cobrix/))
- [CSV](#csv) (see [rfc4180](https://tools.ietf.org/html/rfc4180))
- [FixedWidth](#fixed-width) (see `Link to be added repo does not exist yet`)
- [JSON](#json) (see [json.org](https://www.json.org/json-en.html))
- [Parquet](#parquet) (see [Apache Parquet](https://parquet.apache.org/documentation/latest/))
- [XML](#xml) (see [xml.com](https://www.xml.com/))

When running standardization one of the formats of the list has to be specified.

```shell
...standardization options...
--format <format>
--format-specific-optionX valueY
```

## Cobol

Cobol `format` value is `cobol`. Format options are

| Option | Values domain | Description | Default |
|---|---|---|---|
| copybook | String | Path to a copybook for COBOL data format | - |
| is-xcom | Boolean | Does a mainframe file in COBOL format contain XCOM record headers | false |
| cobol-is-text | Boolean | Specifies if the mainframe file is ASCII text file | `false` |
| cobol-encoding | `ascii` or `ebcdic` | Specifies encoding of mainframe files | - |
| cobol-trimming-policy | `none`, `left`, `right`, `both` | Specify string trimming policy for mainframe files | `none` |
| charset | Any valid charset name | The character set of the input. | `UTF-8` |

## CSV

CSV `format` value is `csv`. Format options are

| Option | Values domain | Description | Default |
|---|---|---|---|
| header | Boolean | Specifies if the input data have a CSV style header | `false` |
| delimiter | Any char or unicode such as `U+00A1` | Delimiter the column values on a row | `,` |
| csv-quote | Any char | Quote character. Delimiters inside quotes are ignored. | `"` |
| csv-escape | Any char | Escape character. Escaped quote characters are ignored. | `\` |
| charset | Any valid charset names | The character set. | `UTF-8` |

## Fixed Width

Fixed Width is a custom in house made format. Requires width metadata, more in [Usage - Schema]({{ docs_path }}/usage/schema#width).

Fixed Width `format` value is `fixed-width`. Format options are

| Option | Values domain | Description | Default |
|---|---|---|---|
| trimValues | Boolean | Uses Java's String `.trim`  method. Removes whitespaces from left and right ends. Required if data is to be casted to any Numeric | `false` |

## JSON

JSON `format` value is `json`. Format options are

| Option | Values domain | Description | Default |
|---|---|---|---|
| charset | Any valid charset names | The character set. | `UTF-8` |

## Parquet

Has no extra options. Only `--format parquet`.

## XML

XML `format` value is `xml`. Format options are

| Option | Values domain | Description | Default |
|---|---|---|---|
| rowTag | String | The tag of the xml file to treat as a row. For example, in the following xml `<books> <book><book> ...</books>`, the appropriate value would be `book`. | - |
| charset | Any valid charset names | The character set. | `UTF-8` |
