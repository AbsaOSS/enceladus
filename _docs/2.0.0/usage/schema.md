---
layout: docs
title: Usage - Schema
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
---
## Table Of Contents
<!-- toc -->
- [Table Of Contents](#table-of-contents)
- [Intro](#intro)
- [Automatically added columns](#automatically-added-columns)
- [Data types](#data-types)
  - [String](#string)
  - [Boolean](#boolean)
  - [Decimal](#decimal)
  - [Long](#long)
  - [Integer](#integer)
  - [Short](#short)
  - [Byte](#byte)
  - [Double](#double)
  - [Float](#float)
  - [Timestamp](#timestamp)
  - [Date](#date)
  - [Binary](#binary)
  - [Struct](#struct)
  - [Array](#array)
- [Metadata](#metadata)
  - [sourcecolumn](#sourcecolumn)
  - [default](#default)
  - [pattern](#pattern)
  - [timezone](#timezone)
  - [decimal_separator](#decimal_separator)
  - [grouping_separator](#grouping_separator)
  - [minus_sign](#minus_sign)
  - [allow_infinity](#allow_infinity)
  - [radix](#radix)
  - [encoding](#encoding)
  - [width](#width)
- [Parsing](#parsing)
  - [Parsing timestamps and dates](#parsing-timestamps-and-dates)
    - [Time Zone support](#time-zone-support)
  - [Parsing numbers](#parsing-numbers)
    - [Radix usage](#radix-usage)
    - [Pattern parsing](#pattern-parsing)
    - [Number parsing peculiarities](#number-parsing-peculiarities)
- [Defaults](#defaults)
  - [Explicit default](#explicit-default)
  - [Global default values](#global-default-values)
  - [Explicit default values restrictions](#explicit-default-values-restrictions)
<!-- tocstop -->

## Intro

Schema is the description of fields in a dataset. All and only the fields defined in the schema will be in the output
table. That means fields not mentioned in the schema won't be in the input. There's an exception of three fields added
automatically - [see bellow](#TODO). Fields are defined in the order they are to be in the output table and have three basic common properties:

- `name` - the field (column) name
- `type` - data type of the field
- `nullable` (optional) - flag indicating if the data can contain the value *null*, if not specified considered set to *false*

Furthermore, some type can have additional properties. The details of each supported type, their meaning and additional
properties will be described in the following chapters.

Thanks to *Data Types* `StructType` and `ArrayType` the fields can be nested –
fields within fields.

You provide *Schema* to **Standardization** in a JSON file:

```json
{
  "type": "struct",
  "fields": [{
      "name": "name",
      "type": "string",
      "nullable": false,
      "metadata": {}
    },
    {
      "name": "surname",
      "type": "string",
      "nullable": false,
      "metadata": {
        "default": "Unknown Surname"
      }
    },
    {
      "name": "hoursWorked",
      "type": {
        "type": "array",
        "elementType": "integer",
        "containsNull": false
      },
      "nullable": false,
      "metadata": {}
    },
    {
      "name": "employeeNumbers",
      "type": {
        "type": "array",
        "elementType": {
          "type": "struct",
          "fields": [{
              "name": "numberType",
              "type": "string",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "numbers",
              "type": {
                "type": "array",
                "elementType": "integer",
                "containsNull": true
              },
              "nullable": true,
              "metadata": {}
            }
          ]
        },
        "containsNull": true
      },
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "startDate",
      "type": "date",
      "nullable": false,
      "metadata": {
        "pattern": "yyyy-MM-dd"
      }
    },
    {
      "name": "updated",
      "type": "timestamp",
      "nullable": true,
      "metadata": {
        "pattern": "yyyyMMdd.HHmmss"
      }
    }
  ]
}
```

Example of data adhering to the above schema can be found [here][test-samples].

## Automatically added columns

There is a column automatically added to each **Standardization** output. Its name is `errCol` and it contains information
on all errors that happened on the particular row *standardization*. If defined in schema its structure there has to 
adhere exactly to the automatically added one. More on this field [see in dedicated documentation]({{ site.baseurl }}/docs/{{ page.version }}/usage-errcol).  

## Data types

### String

The data type representing *String* values.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [width](#width)</sup>

### Boolean

The data type representing *Boolean* values.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [width](#width)</sup>

### Decimal

The data type representing *BigDecimal* values, a fixed-point numeric type. The type is specified by two additional 
parameters, *precision* and *scale*. *Precision* limits the number of digits and cannot be greater than 38. *Scale* 
specifies the number of digits after the decimal point and has to be equal or less than the *precision*.

The type is specified as `decimal(`*precision*, *scale*`)`, for example: `decimal(15, 3)`

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [timezone](#timezone), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [width](#width)</sup>

### Long

The data type representing *Long* values. That is a whole number between -9223372036854775808 and 9223372036854775807.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [radix](#radix), [width](#width)</sup>

### Integer

The data type representing *Integer* values. That is a whole number between -2147483648 and 2147483647.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [radix](#radix), [width](#width)</sup>

### Short

The data type representing *Short* values. That is a whole number between  -32768 and 32767.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [radix](#radix), [width](#width)</sup>

### Byte

The data type representing *Byte* values. That is a whole number between -128 and 127.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [radix](#radix), [width](#width)</sup>

### Double

The data type representing *Double* values, 64-bit (IEEE 754) double-precision float.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [allow_infinity](#allow_infinity), [width](#width)</sup>

### Float

The data type representing *Float* values, 32-bit (IEEE 754) single-precision float.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [allow_infinity](#allow_infinity), [width](#width)</sup>

### Timestamp

The data type representing *java.sql.Timestamp* values. Upon entry they are normalized to UTC time zone.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [timezone](#timezone), [width](#width)</sup>

### Date

The data type representing *java.sql.Date* values. If time zone is specified the date is adjusted to UTC.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [timezone](#timezone), [width](#width)</sup>

### Binary

The data type representing *Binary* values.

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn), [default](#default), [encoding](#encoding), [width](#width)</sup>

### Struct

The data type representing a structure of one or more sub-fields.

The type is specified as struct of the following properties:

- `type` - string value *"array"*
- `fields` - array of fields

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn)</sup>

### Array

The data type representing an array of values of another type.

The type is specified as struct of following properties:

- `type` - string value *"array"*
- `elementType` - the type of the elements of the array, can be any of the types including [`struct`](#struct) and
[`array`](#array)
- `containsNull`- boolean value

<sup>**Metadata keys:** [sourcecolumn](#sourcecolumn)</sup>

## Metadata

*Standardization* can be influenced by `metadata` in the schema of the data. The `metadata` are optional properties.
Here are the recognized ones with the description of their purpose (with detailed description below):

| Property                                  | Target data type            | Description                                                                                                                                           | Example         | Default[\*](#metadata-star)                  |
|-------------------------------------------|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|----------------------------------------------|
| [sourcecolumn](#sourcecolumn)             | any                         | The source column to provide data of the described column                                                                                             | *id*            | `-`[\*\*](#metadata-star-star)               |
| [default](#default)                       | any atomic type             | Default value to use in case data are missing                                                                                                         | *0*             | `-`[\*\*](#metadata-star-star)               |
| [pattern](#pattern)                       | timestamp & date            | Pattern for the timestamp or date representation                                                                                                      | *dd.MM.yy*      | *yyyy-MM-dd HH:mm:ss* **/** *yyyy-MM-dd*     |
| [timezone](#timezone)                     | timestamp (also date)       | The time zone of the timestamp when that is not part of the pattern (NB! for date it can return unexpected results)                                   | *US/Pacific*    | *UTC*[\*\*\*](#metadata-star-star-star)      |
| [pattern](#pattern)                       | any numeric type            | Pattern for the number representation                                                                                                                 | \#,\#\#0.\#     | `-`[\*\*](#metadata-star-star)               |
| [decimal_separator](#decimal_separator)   | any numeric type            | The character separating the integer and the fractional parts of the number                                                                           | *,*             | *.*                                          |
| [grouping_separator](#grouping_separator) | any numeric type            | Character to mark boundaries between orders of magnitude, usually to mark thousands, millions etc.                                                    | *\_*            | *,*                                          |
| [minus_sign](#minus_sign)                 | any numeric type            | Character to mark the number is negative.                                                                                                             | *N*             | *-*                                          |
| [allow_infinity](#allow_infinity)         | float & double              | Flag indicating if the column accepts infinity as a value (and positive/negative numbers which are too large are converted to *infinity*/*-infinity*) | *true*          | *false*                                      |
| [strict_parsing](#strict_parsing)         | decimal                     | Flat indicating strict parsing on the provided decimal value. Strict parsing rejects a value(including default) with a larger scale than allowed      | *true*          | *false*                                      |
| [radix](#radix)                           | long, integer, short, byte  | The base of the numbers provided                                                                                                                      | *hex*           | *10*                                         |
| [encoding](#encoding)                     | binary                      | Encoding is used for string to binary conversion                                                                                                      | *base64*,*none* | `-` (explained in [encoding](#encoding))     |
| [width](#width) | any atomic type | Specifies the width of a column for a fixed-width formats | "10" | - |

**NB!** All values in _metadata_ have to be entered as *string*. Even if they would conform to other types, like number
or boolean.

<a id="metadata-star" />\* Value used if nothing is specified in _metadata_

<a id="metadata-star-star" />\*\* No default exists (as not needed)

<a id="metadata-star-star-star" />\*\*\* Unless a different default time zone is specified via
`defaultTimestampTimeZone` and `defaultDateTimeZone` application settings

### sourcecolumn

<sup>**Supported by types:** [String](#string), [Boolean](#boolean), [Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float), [Timestamp](#struct), [Date](#timestamp), [Struct](#struct), [Array](#array)</sup>

The name of the column to get the data from (so it only makes sense if it's different
from field name). The most common use case is when the original column
name is not a valid Parquet field name. It can also be used in the rare cases
when the column needs to be standardized into more fields and/or different types.

### default

<sup>**Supported by types:** [String](#string), [Boolean](#boolean), [Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float), [Timestamp](#struct), [Date](#timestamp)</sup>

This is the value to be used in case the input is missing (and nulls are not
allowed) or when the casting (*standardization*) fails.
You can think of this as a *fallback value*.

It should be noted, that this is the only _metadata_ key which accepts the `null` value (written without quotes) next to
string values (of course, such a field has to be nullable: `"nullable": true`)

For more about the topic see chapter [Defaults](#defaults).

### pattern

<sup>**Supported by types:** [Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float), [Timestamp](#struct), [Date](#timestamp)</sup>

The format the input adheres to. Mostly used for timestamp and date entries but
it can be leveraged for numeric types too. Details for valid patterns are in
the chapter [Parsing](#parsing).

In case ``default`` value is  specified in _metadata_, it needs to adhere to
the pattern.

If [radix](#radix) is specified and differs from the default 10, `pattern` value
will be ignored.

### timezone

<sup>**Supported by types:** [Timestamp](#struct), [Date](#timestamp)</sup>

Time zone of the timestamp or date (not recommended for the latter). For details see the chapter
[Parsing timestamps and dates](#parsing-timestamps-and-dates).

In case the [`pattern`](#pattern) already includes information to recognize the time zone, the `timezone` entry in _metadata_ will
be ignored. Namely if the pattern includes the *"z"*, *"Z"* or *"X"* placeholder or the *"epoch"*, *"epochmilli"*,
*"epochmicro"* and *"epochnano"* keywords.

**NB!** Due to a Spark limitation, only time zone IDs are accepted as valid values. To get the full list of supported time
 zone denominators see the output of Java's
[`TimeZone.getAvailableIDs()` function][oracle-tz-ids].

### decimal_separator

<sup>**Supported by types:** [Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float)</sup>

The character separating the integer and the fractional parts of the number.

For whole numbers which use *"."* as the [`grouping_separator`](#grouping_separator), the `decimal_separator` has to be
redefined, to avoid conflict.

### grouping_separator

<sup>**Supported by types:** [Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float)</sup>

Character to mark boundaries between orders of magnitude, usually to mark
thousands, millions etc.

It has to be used in the pattern to be taken into consideration.

### minus_sign

<sup>**Supported by types:** [Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float)</sup>

Character to mark the number is negative. By default, it’s the standard minus
sign (*"-"*).

### allow_infinity

<sup>Supported by types:** [Double](#double), [Float](#float)</sup>

Flag indicating if the column accepts infinity as a value. When set to true *infinity*/*-infinity* are recognized as a 
valid value, instead of failing with casting error ([see here]({{ site.baseurl }}/docs/{{ page.version }}/usage-errcol)).
The string representing infinity on input is *"∞"* and *"-∞"* respectively. Positive and negative numbers with values 
that are too large are converted to *infinity* and *-infinity*, respectively.

### strict_parsing

<sup>Supported by types:** [Decimal](#decimal)</sup>

Flat indicating strict parsing on the provided decimal value.
Strict parsing rejects a value(including default) with a larger scale than allowed with casting error
([see here]({{ site.baseurl }}/docs/{{ page.version }}/usage-errcol)). For example for Decimal(X,2), the values with
longer scale(like 10.12345, 0.1234) will rejected 

### radix

<sup>**Supported by types:** [Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte)</sup>

The radix (base) of the numbers entered. Accepted values are numbers between 1
and 36, as well as the following keywords (case insensitive): *"dec"*,
*"decimal"*, *"hex"*, *"hexadecimal"*, *"bin"*, *"binary"*, *"oct"*, *"octal"*.

For higher bases, letters (A, B, C etc.) are used for digits (case insensitive).

For hexadecimal value entries in the form *"0xFF"* are accepted as well.

If `radix` is specified as anything other than the default 10, [pattern](#pattern)
value will be ignored.

### encoding

<sup>**Supported by types:** [Binary](#Binary)</sup>

**When a string value is being converted to binary**, the supplied `encoding` indicates how the values are going to be 
treated. This applies for the default value, too:
 - `none` - the input will get cast as-is to binary. E.g. "abc" -> [97, 98, 99]
 - `base64` - the input is considered as Base64-encoded and will get unencoded. Contrary to the basic Spark behavior of
 `unbase64` (which skips characters invalid for Base64), this will result in an error.

If `encoding` is missing altogether when it would be needed (e.g. when default value is given), `ValidationWarning` is
 issued and the encoding value is considered to be `none`.
 
`encoding` is not considered if BinaryType is already found in the input (no conversion is happening there).

### width

<sup>**Supported by types:** [String](#string), [Boolean](#boolean), [Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float), [Timestamp](#struct), [Date](#timestamp)</sup>

Specifically for the Fixed-Width data format. Specifies the width of the column.

## Parsing

### Parsing timestamps and dates

Dates and especially timestamps (date + time) can be tricky. Currently Spark considers all time entries to be in the
system's time zone by default. (For more detailed explanation of possible issues with that see
[Consistent timestamp types in Hadoop SQL engines][timestamp-types].)

To address this potential source of discrepancies the following has been implemented:

1. All Enceladus components are set to run in UTC
1. As part of **Standardization** all time related entries are normalized to UTC
1. There are several methods how to ensure that a timestamp entry is normalized as expected
1. We urge users, that all timestamp entries should include time zone information in one of the supported ways
1. While this is all valid for date entries too, it should be noted that UTC normalization of a date can have unexpected
consequences - namely all dates west from UTC would be shifted to a day earlier

To enable processing of time entries from other systems **Standardization** offers the possibility to convert
string and even numeric values to timestamp or date types. This is done using Spark's ability to convert strings to
timestamp/date with some enhancements. The pattern placeholders and usage is described in Java's
[`SimpleDateFormat` class description][oracle-simple-date-format] with
the addition of recognizing some keywords (like `epoch` and `milliepoch` (case insensitive)) to denote the number of
seconds/milliseconds since epoch (1970/01/01 00:00:00.000 UTC) and some additional placeholders.
It should be noted explicitly that *"epoch"*, *"epochmilli"*, *"epochmicro"* and *"epochnano"* are considered a pattern
including time zone.

Summary:

| placeholder                                | Description                                      | Example                                |
|--------------------------------------------|--------------------------------------------------|----------------------------------------|
| `G`                                        | Era designator                                   | AD                                     |
| `y`                                        | Year                                             | 1996; 96                               |
| `Y`                                        | Week year                                        | 2009; 09                               |
| `M`                                        | Month in year (context sensitive)                | July; Jul; 07                          |
| `L`                                        | Month in year (standalone form)                  | July; Jul; 07                          |
| `w`                                        | Week in year                                     | 27                                     |
| `W`                                        | Week in month                                    | 2                                      |
| `D`                                        | Day in year                                      | 189                                    |
| `d`                                        | Day in month                                     | 10                                     |
| `F`                                        | Day of week in month                             | 2                                      |
| `E`                                        | Day name in week                                 | Tuesday; Tue                           |
| `u`                                        | Day number of week (1 = Monday, ..., 7 = Sunday) | 1                                      |
| `a`                                        | Am/pm marker                                     | PM                                     |
| `H`                                        | Hour in day (0-23)                               | 0                                      |
| `k`                                        | Hour in day (1-24)                               | 24                                     |
| `K`                                        | Hour in am/pm (0-11)                             |  0                                     |
| `h`                                        | Hour in am/pm (1-12)                             | 12                                     |
| `m`                                        | Minute in hour                                   | 30                                     |
| `s`                                        | Second in minute                                 | 55                                     |
| `S`                                        | Millisecond                                      | 978                                    |
| `z`                                        | General time zone                                | Pacific Standard Time; PST; GMT-08:00  |
| `Z`                                        | RFC 822 time zone                                | -0800                                  |
| `X`                                        | ISO 8601 time zone                               | -08; -0800; -08:00                     |
| `_epoch_`                                  | Seconds since 1970/01/01 00:00:00                | 1557136493, 1557136493.136             |
| `_epochmilli_`                             | Milliseconds since 1970/01/01 00:00:00.0000      | 1557136493128, 1557136493128.001       |
| `_epochmicro_`                             | Microseconds since 1970/01/01 00:00:00.0000      | 1557136493128789, 1557136493128789.999 |
| `_epochnano_`<sup>[*](#parsing-star)</sup> | Nanoseconds since 1970/01/01 00:00:00.0000       | 1557136493128789101                    |
| `i`                                        | Microsecond                                      | 111, 321001                            |
| `n`<sup>[*](#parsing-star)</sup>           | Nanosecond                                       | 999, 542113879                         |

<a id="parsing-star" />\* While _nanoseconds_ designation is supported on input, it's not supported in storage or further usage. So any
   value behind microseconds precision will be truncated.

**NB!** Spark uses US Locale and because on-the-fly conversion would be complicated, at the moment we stick to this
hardcoded locale as well. E.g. `am/pm` for `a` placeholder, English names of days and months etc.

**NB!** The keywords are case **insensitive**. Therefore, there is no difference between `epoch` and `EpoCH`.

#### Time Zone support

As mentioned, it is highly recommended to use timestamps with time zone but it's not unlikely that the
source for standardization doesn't provide time zone information. On the other hand, these times are usually within
one time zone. To ensure proper standardization, the schema's _metadata_ can include the `timezone` value.
When set, all timestamps in the column will be standardized as belonging to the particular time zone.

E.g. _2019-05-04 11:31:10_ with `timzone` specified as _CET_ will be standardized to _2019-05-04 10:31:10_ (UTC of
course)

### Parsing numbers

When converting *string* to any of the numeric types there are two standard formats accepted:

1. the usual string of digits with the eventual minus or plus sign in front and optional decimal separator (e.g *3.14*)
1. the scientific notation, where the numbers are expressed as the product of a mantissa and a power of ten
(e.g. 1234 can be expressed as 1.234 x 10^3 = 1.234E3)

Note, that for whole numbers ([Long](#long), [Integer](#integer), [Short](#short) and [Byte](#byte)), the decimal
separator must not be present.

If the string is being parsed to [decimal type](#decimal) and the input has more decimal places than is the *scale* of
the decimal type, the result will be rounded to the number of decimal places allowed by *scale*.

#### Radix usage

For whole numbers, the numbers can be entered using a different radix (base) than the usual 10. For radices smaller than
10, the appropriate subset of numeric digits are accepted. For radices above 10, letters are used for the digit
representation. The letters are case insensitive, therefore 1Fa = 1fA.

To specify a non-standard radix (different from 10), use the [`Radix` *metadata* key](#radix). The radix has to be between 1
and 36.

#### Pattern parsing

When the number is formatted in some non-standard way you can use a pattern. The parsing is executed using
the *Java* class `DecimalFormat`, whose [documentation][oracle-decimal-format]
provides the most comprehensive explanation of patterns and their usage.

Pattern contains a positive and negative subpattern, for example, `#,##0.00;(#,##0.00)`. Each subpattern has a prefix,
numeric part, and suffix. The negative subpattern is optional; if absent, the positive subpattern prefixed with the
minus sign ('-' in most locales) is used as the negative subpattern. That is, `0.00` alone is equivalent to `0.00;-0.00`.
If there is an explicit negative subpattern, it serves only to specify the negative prefix and suffix; the number of
digits, minimal digits, and other characteristics are all the same as the positive pattern. That means that `#,##0.0#;(#)`
produces precisely the same behavior as `#,##0.0#;(#,##0.0#)`.

The prefixes, suffixes, and various symbols used for infinity, digits, thousands separators, decimal separators, etc. may
be set to arbitrary values. However, care must be taken that the symbols and strings do not conflict, or parsing will be
unreliable. For example, either the positive and negative prefixes or the suffixes must be distinct for parsing to be able
to distinguish positive from negative values. Another example is that the decimal separator and thousands separator should
be distinct characters, or parsing will be impossible.

| Symbol | Location              | Meaning                                                                                                                                                                                                             | Metadata to change                                                                 |
|--------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `0`    | Number                | Digit                                                                                                                                                                                                               |                                                                                    |
| `#`    | Number                | Digit, zero shows as absent                                                                                                                                                                                         |                                                                                    |
| `.`    | Number                | Decimal separator                                                                                                                                                                                                   | [Decimal separator](#deciaml_separator) NB!<sup>[*](#pattern-parsing-star)</sup>   |
| `-`    | Number                | Minus sign                                                                                                                                                                                                          | [Minus sign](#minus_sign) NB!<sup>[*](#pattern-parsing-star)</sup>                 |
| `,`    | Number                | Grouping separator                                                                                                                                                                                                  | [Grouping separator](#grouping_separator) NB!<sup>[*](#pattern-parsing-star)</sup> |
| `E`    | Number                | Separates mantissa and exponent in scientific notation. Need not be quoted in prefix or suffix.                                                                                                                     |                                                                                    |
| `;`    | Subpattern boundary   | Separates positive and negative subpatterns                                                                                                                                                                         |                                                                                    |
| `%`    | Prefix or suffix      | Divide by 100 on parsing                                                                                                                                                                                            |                                                                                    |
| `‰`    | Prefix or suffix      | Divide by 1000 on parsing                                                                                                                                                                                           |                                                                                    |
| `'`    | Prefix or suffix      | Used to quote special characters in a prefix or suffix, for example, the "'#'#" pattern allows the value `#123` to be read in as the number `123`. To create a single quote itself, use two in a row: "# o''clock". |                                                                                    |
| `∞`    | *not part of pattern* | String to represent infinity                                                                                                                                                                                        |                                                                                    |

<a id="pattern-parsing-star"/>\* while these can be changed, in the [`pattern`]($pattern) the default value *must* be 
used. E.g. *"."*, *"."* and *"-"*.

**NB!** If there's no special format of the input, it's advised to avoid the usage of patterns due to the added
performance hit (parsing using a pattern adds computational overhead). It's still possible to redefine the special
symbols like [decimal separator](#decimal_separator) or [minus sign](#minus_sign) even without the pattern.

#### Number parsing peculiarities

- number of `#` and `0` in the pattern is not reliable, the input is parsed regardless (e.g. *"0.0"* produces the same
result as *"##.#*)
- Grouping separator placement is "flexible". If the grouping separator has been added to the pattern the number is
parsed regardless of the placement(s) of the grouping separator in the parsed strings (e.g. pattern `#,##0.#` will parse
_1,000.0_, _1,1234.5_ so as  _2000._)
- Grouping separator is not accepted in decimal places
- _"+"_ is not accepted when pattern is specified, unless explicitly present in the pattern
- `%` and `‰` in fractional and decimal values and patterns divide the number by 100 and 1000 (in integrals as well, but
then they are usually not whole numbers)
- Even if redefined the standard grouping and decimal separators **need to be used** in the pattern
- If pattern is used, `e` is not accepted only `E` in the exponential expresion (without a pattern both are recognized)

## Defaults

As described, when a field fails to standardize, either because of missing data in a non-nullable column
or because it was being cast to the wrong type, the field is populated with a default value and an error is added to 
the [error column](({{ site.baseurl }}/docs/{{ page.version }}/usage-errcol)).

### Explicit default

The value optionally specified in the schema of the dataset for **Standardization** in _metadata_ [Default](#default)
property to be used for the particular column.

### Global default values

The value used when _explicit default_ was not defined in the schema:

- `null` for nullable column
- `0` for numeric column
- `false` for Boolean column
- `""` (empty string) for string column
- `Array.empty[Byte]` (empty array of bytes) for binary column 
- `1970/01/01` for date column
- `1970/01/01 00:00:00` for timestamp column

- default timezone if not specified for both timestamps and dates is *UTC*; this can be changed via application settings
`defaultTimestampTimeZone` and `defaultDateTimeZone`
- default locale and the decimal symbols based on that is *US*. Therefore:
  - minus sign is *"-"*
  - decimal separator is *"."*
  - grouping separator is *","*

### Explicit default values restrictions

- The value has to be a string convertible to the field's type and fitting within its size limitations (e.g._"200"_
cannot be a `default` for the type [`Short`](#short), or _"∞"_ if `allow_infinity` is _"false"_ for [`Double`](#double)/
[`Float`](#float))
- If it's a type supporting [`pattern`](#pattern) and it is defined, the default value has to adhere to the `pattern`

[test-samples]: https://github.com/AbsaOSS/enceladus/blob/master/spark-jobs/src/test/scala/za/co/absa/enceladus/standardization/samples/TestSamples.scala
[oracle-tz-ids]: https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html#getAvailableIDs--
[timestamp-types]: https://docs.google.com/document/d/1gNRww9mZJcHvUDCXklzjFEQGpefsuR_akCDfWsdE35Q/edit#heading=h.n699ftkvhjlo
[oracle-simple-date-format]: https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
[oracle-decimal-format]: https://docs.oracle.com/javase/7/docs/api/java/text/DecimalFormat.html
