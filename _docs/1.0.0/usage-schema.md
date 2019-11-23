---
layout: docs
title: Usage
version: '1.0.0'
categories:
    - '1.0.0'
---
Schema
======

<!-- toc -->
- [Intro](#intro)

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

  - [Timestamp](#struct)

  - [Date](#timestamp)

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

- [Parsing](#parsing)

  - [Parsing timestamps and dates](#parsing-timestamps-and-dates)

  - [Parsing numbers](#parsing-numbers)

- [Defaults](#defaults)

  - [Explicit default](#explicit-default)

  - [Global default values](#global-default-values)

  - [Local default values restrictions](#explicit-default-values-restrictions)
<!-- tocstop -->

Intro
-----

Schema is the description of fields in a dataset. Fields are defined in the
order they are to be in the output table and have three basic common properties:

- Field name: `name`

- Field data type: `type`

- Flag if the data can contain the value null (optional, if not specified considered false): `nullable`

Furthermore, some type can have additional properties. The details of each supported type, their meaning and additional
properties will be described in the following chapters.

Thanks to *Data Types* `StructType` and `ArrayType` the fields can be nested –
fields within fields.

The mean to provide *Schema* to **Standardization** is a JSON file:

```yaml
{
    "type": "struct",
    "fields": [{
        "name": "name",
        "type": "string",
        "nullable": false,
        "metadata": {
        }
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
        "metadata": {
        }
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
                    "metadata": {
                    }
                },
                {
                    "name": "numbers",
                    "type": {
                        "type": "array",
                        "elementType": "integer",
                        "containsNull": true
                    },
                    "nullable": true,
                    "metadata": {
                    }
                }]
            },
            "containsNull": true
        },
        "nullable": true,
        "metadata": {
        }
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

Data types
----------

### String

The data type representing *String* values.

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default)</sup></sub> |
|--------------------------------------|--------------------------------------------------------------------------|
| | |

### Boolean

The data type representing *Boolean* values.

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default)</sup></sub> |
|--------------------------------------|--------------------------------------------------------------------------|
| | |

### Decimal

The data type representing *BigDecimal*, the fixed point, values. The type is specified by two additional parameteres,
*precision* and *scale*. *Precision* limits the number of digits and cannot be bigger then 38. *Scale* specifies the
number of decimal points of the number and have to be equal or smaller then *precision*.

The type is specified as `decimal(`*precision*, *scale*`)`, for example: `decimal(15, 3)`

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [timezone](#timezone), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign)</sup></sub> |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

### Long

The data type representing *Long* values. That is a whole number between -9223372036854775808 and 9223372036854775807.

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [radix](#radix)</sup></sub> |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

### Integer

The data type representing *Integer* values. That is a whole number between -2147483648 and 2147483647.

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [radix](#radix)</sup></sub> |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

### Short

The data type representing *Short* values. That is a whole number between  -32768 and 32767.

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [radix](#radix)</sup></sub> |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

### Byte

The data type representing *Byte* values. That is a whole number between -128 and 127.

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [radix](#radix)</sup></sub> |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

### Double

The data type representing *Double* values, 64 bit IEEE 754 double-precision float.

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [allow_infinity](#allow_infinity)</sup></sub> |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

### Float

The data type representing *Float* values, 32 bit IEEE 754 single-precision float.

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [decimal_separator](#decimal_separator), [grouping_separator](#grouping_separator), [minus_sign](#minus_sign), [allow_infinity](#allow_infinity)</sup></sub> |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

### Timestamp

The data type representing *java.sql.Timestamp* values. Upon entry they are normalized to UTC time zone.

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [timezone](#timezone)</sup></sub> |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| | |

### Date

The data type representing *java.sql.Date9 values. If time zone is specified the date is adjusted to UTC.

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn), [default](#default), [pattern](#pattern), [timezone](#timezone)</sup></sub> |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| | |

### Struct

The data type representing a structure of one or more sub-fields.

The type is specified as struct of following properties:

- `type` - string value *"array"*
- `fields` - array of fields

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn)</sup></sub> |
|--------------------------------------|-----------------------------------------------------|
| | |

### Array

The data type representing an array of values of another type.

The type is specified as struct of following properties:

- `type` - string value *"array"*
- `elementType` - the type of the elements of the array, can be any of the types including [`struct`](#struct) and
[`array`](#array)
- `containsNull`- boolean value

| <sub><sup>Metadata keys:</sup></sub> | <sub><sup>[sourcecolumn](#sourcecolumn)</sup></sub> |
|--------------------------------------|-----------------------------------------------------|
| | |

Metadata
--------

*Standardization* can be influenced by `metadata` in the schema of the data.
They are non-mandatory additional properties. Here are the recognized ones with
the description of their purpose (with detailed description bellow):

| Property                                  | Target data type           | Description                                                                                                                                  | Example      | Default[\*](#metadata-star)              |
|-------------------------------------------|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|--------------|------------------------------------------|
| [sourcecolumn](#sourcecolumn)             | any                        | The source column to provide data of the described column                                                                                    | *id*         | `-`[\*\*](#metadata-star-star)            |
| [default](#default)                       | any atomic type            | Default value to use in case data are missing                                                                                                | *0*          | `-`[\*\*](#metadata-star-star)            |
| [pattern](#pattern)                       | date & timestamp           | Pattern for the date or timestamp representation                                                                                             | *dd.MM.yy*   | *yyyy-MM-dd HH:mm:ss* **/** *yyyy-MM-dd* |
| [timezone](#timezone)                     | timestamp (also date)      | The time zone of the timestamp when that is not part of the pattern (NB! for date it can return unexpected results)                          | *US/Pacific* | *UTC*[\*\*\*](#metadata-star-star-star)    |
| [pattern](#pattern)                       | any numeric type           | Pattern for the number representation                                                                                                        | \#,\#\#0.\#  | `-`[\*\*](#metadata-star-star)            |
| [decimal_separator](#decimal_separator)   | any numeric type           | The character separating the integer and the fractional parts of the number                                                                  | *,*          | *.*                                      |
| [grouping_separator](#grouping_separator) | any numeric type           | Character to mark boundaries between orders of magnitude, usually to mark thousands, millions etc.                                           | *\_*         | *,*                                      |
| [minus_sign](#minus_sign)                 | any numeric type           | Character to mark the number is negative.                                                                                                    | *N*          | *-*                                      |
| [allow_infinity](#allow_infinity)         | float & double             | Flag indicating if the column accepts infinity as a value (also too big/far negative numbers are converted to become *infinity*/*-infinity*) | *true*       | *false*                                  |
| [radix](#radix)                           | long, integer, short, byte | The base of the numbers provided                                                                                                             | *hex*        | *10*                                     |

**NB!** All values in _metadata_ have to be entered as *string*. Even in cases
they would conform to other types, like number or Boolean.

<a name="#metadata-star" />\* Value used if nothing is specified in _metadata_

<a name="#metadata-star-star" />\*\* No default exists (as not needed)

<a name="#metadata-star-star-star" />\*\*\* Unless a different default time zone is specified via
`defaultTimestampTimeZone` and `defaultDateTimeZone` application settings

### sourcecolumn

| <sub><sup>Supported by types:</sup></sub> | <sub><sup>[String](#string), [Boolean](#boolean), [Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float), [Timestamp](#struct), [Date](#timestamp), [Struct](#struct), [Array](#array)</sup></sub> |
|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

The name of the column to get the data from (so have a point only if it differs
from field name). The most common use is for the case, when the original column
name is not a valid Parquet field name. It can be also used in the rate cases,
when the column needs to be standardized into more fields and/or different
types.

### default

| <sub><sup>Supported by types:</sup></sub> | <sub><sup>[String](#string), [Boolean](#boolean), [Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float), [Timestamp](#struct), [Date](#timestamp)</sup></sub> |
|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

This is the value to be used in case the input is missing (and nulls are not
allowed) or when the casting (*standardization*) fails. So better
characterization would be, that it’s a *fallback value*.

Should be noted, that this is the only _metadata_ key recognized that accepts the
`null` value (written like that without quotes) next to string values.

For more about the topic see chapter [Defaults](#defaults).

### pattern

| <sub><sup>Supported by types:</sup></sub> | <sub><sup>[Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float), [Timestamp](#struct), [Date](#timestamp)</sup></sub> |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

The format the input adheres to. Mostly used for timestamp and date entries. But
it can be also leveraged in case of numbers. Details for valid patterns are in
the chapter [Parsing](#parsing).

In case ``default`` value is also specified in _metadata_, it needs to adhere to
the pattern.

If [radix](#radix) is specified and differs from the default 10, `pattern` value
will be ignored.

### timezone

| <sub><sup>Supported by types:</sup></sub> | <sub><sup>[Timestamp](#struct), [Date](#timestamp)</sup></sub> |
|-------------------------------------------|----------------------------------------------------------------|

Time zone of the timestamp or date (for later it's generally not recommended). For details see the chapter
[Parsing timestamps and dates](#parsing-timestamps-and-dates).

In case the [`pattern`](#pattern) already includes information to recognize the time zone, the `timezone` entry in _metadata_ will
be ignored. Namely if the pattern includes *"z"*, *"Z"* or *"X"* placeholder or *"epoch"*, *"epochmilli"*, *"epochmicro"*
 and *"epochnano"* keywords.

**NB!** Due to spark limitation, only time zone ids are accepted as valid values. To get the full list of supported time
 zone denominators see the output of Java's
[`TimeZone.getAvailableIDs()` function](https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html#getAvailableIDs--).

### decimal_separator

| <sub><sup>Supported by types:</sup></sub> | <sub><sup>[Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float)</sup></sub> |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

The character separating the integer and the fractional parts of the number.

Seems pointless for whole numbers, but in case *"."* is used for example as
[`grouping_separator`](#grouping_separator) the , the `decimal_separator` has to
be redefined, to avoid conflict.

### grouping_separator

| <sub><sup>Supported by types:</sup></sub> | <sub><sup>[Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float)</sup></sub> |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

Character to mark boundaries between orders of magnitude, usually to mark
thousands, millions etc.

It has to be used in the pattern to be taken into consideration.

### minus_sign

| <sub><sup>Supported by types:</sup></sub> | <sub><sup>[Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte), [Double](#double), [Float](#float)</sup></sub> |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

Character to mark the number is negative. By default, it’s the standard minus
sign (*"-"*).

### allow_infinity

| <sub><sup>Supported by types:</sup></sub> | <sub><sup>[Double](#double), [Float](#float)</sup></sub> |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

Flag indicating if the column accepts infinity as a value. Also too big/far
negative numbers are converted to become *infinity* or *-infinity* respectively,
if the flag is set to true, instead of failing the *standardization*.

The string representing infinity on input is *"∞"* and *"-∞"* respectively.

### radix

| <sub><sup>Supported by types:</sup></sub> | <sub><sup>[Decimal](#decimal), [Long](#long), [Integer](#integer), [Short](#short), [Byte](#byte)</sup></sub> |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| | |

The base (radix) of the numbers entered. Accepted values are numbers between 1
and 36. Also accepted are the following keywords (case insensitive): *"dec"*,
*"decimal"*, *"hex"*, *"hexadecimal"*, *"bin"*`, *"binary"*, *"oct"*, *"octal"*.

For higher bases, letters (A, B, C etc.) are used for digits, case insensitive
again.

Also for hexadecimal value entries in the form *"0xFF"* are accepted.

If `radix` is specified and differs from the default 10, [pattern](#pattern)
value will be ignored.

Parsing
-------

### Parsing timestamps and dates

Dates and especially timestamps (date + time) can be tricky. Currently Spark considers all time entries to be in the
current system time zone by default. (For more detailed explanation of possible issues with that see
[Consistent timestamp types in Hadoop SQL engines](https://docs.google.com/document/d/1gNRww9mZJcHvUDCXklzjFEQGpefsuR_akCDfWsdE35Q/edit#heading=h.n699ftkvhjlo).)

To address this potential source of discrepancies the following has been implemented:

1. All Enceladus components are set to run in UTC
1. As part of **Standardization** all time related entries are normalized to UTC
1. There are several methods how to ensure that a timestamp entry is normalized as expected
1. We urge users, that all timestamp entries should include time zone information in one of the supported ways
1. While this is all valid for date entries too, it should be noted that UTC normalization of a date can have unexpected
consequences - namely all dates west from UTC would be shifted to a day earlier

To enable processing of time entries from other systems **Standardization** offers the possibility to convert
string and even numeric values to timestamp or date types. It's done using Spark's ability to convert strings to
timestamp/date with some enhancements. The pattern placeholders and usage is described in Java's
[`SimpleDateFormat` class description](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html) with
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

<a name="parsing-star" />"\* While _nanoseconds_ designation is supported on input, it's not supported in storage or further usage. So any
   value behind microseconds precision will be truncated.

**NB!** Spark uses US Locale and because on-the-fly conversion would be complicated, at the moment we stick to this
hardcoded locale as well. E.g. `am/pm` for `a` placeholder, English names of days and months etc.

**NB!** The keywords are case **insensitive**. Therefore, there is no difference between `epoch` and `EpoCH`.

#### Time Zone support

As it has been mentioned, it's highly recommended to use timestamps with the time zone. But it's not unlikely that the
source for standardization doesn't provide the time zone information. On the other hand, these times are usually within
one time zone. To ensure proper standardization, the schema's _metadata_ can include the `timezone` value.
All timestamps then will be standardized as belonging to the particular time zone.

E.g. _2019-05-04 11:31:10_ with `timzene` specified as _CET_ will be standardized to _2019-05-04 10:31:10_ (UTC of
course)

### Parsing numbers

When converting *string* to any of the numeric types there are two standard formats accepted:

1) the usual string of digits with the eventual minus or plus sign in front and optional decimal separator (e.g *3.14*)
2) the scientific notation, where the numbers are expressed as the product of a mantissa and a power of ten
(e.g. 1234 can be expressed as 1.234 x 10^3 = 1.234E3)

Note, that for whole numbers ([Long](#long), [Integer](#integer), [Short](#short) and [Byte](#byte)), the decimal
separator mustn't be present.

If the string is being parse to [decimal type](#decimal) and the input has more decimal places than is the *scale* of the
decimal type, the result will be rounded to the number of decimal places allowed by *scale*.

#### Radix usage

For whole numbers , the numbers can be entered using a different radix (base) then the usual 10. For radices smaller then
10, the appropriate subset of numeric digits are accepted. For radices above 10, letters are used for the digit
representation. The letters are case insensitive, therefore 1Fa = 1fA.

To specify a non-standard radix (different from 10), use the [`Radix` *metadata* key](#radix). The radix has to be between 1
and 36.

#### Pattern parsing

For cases when the number is formatted in some other manner, patterns can come for rescue. The parsing is executed using
the *Java* class `DecimalFormat`, whose [documentation](#https://docs.oracle.com/javase/7/docs/api/java/text/DecimalFormat.html)
provides the most comprehensive explanation of patterns and their usage.

Pattern contains a positive and negative subpattern, for example, `#,##0.00;(#,##0.00)`. Each subpattern has a prefix,
numeric part, and suffix. The negative subpattern is optional; if absent, then the positive subpattern prefixed with the
minus sign ('-' in most locales) is used as the negative subpattern. That is, `0.00` alone is equivalent to `0.00;-0.00`.
If there is an explicit negative subpattern, it serves only to specify the negative prefix and suffix; the number of
digits, minimal digits, and other characteristics are all the same as the positive pattern. That means that `#,##0.0#;(#)`
produces precisely the same behavior as `#,##0.0#;(#,##0.0#)`.

The prefixes, suffixes, and various symbols used for infinity, digits, thousands separators, decimal separators, etc. may
be set to arbitrary values. However, care must be taken that the symbols and strings do not conflict, or parsing will be
unreliable. For example, either the positive and negative prefixes or the suffixes must be distinct for parsing to be able
to distinguish positive from negative values. Another example is that the decimal separator and thousands separator should
be distinct characters, or parsing will be impossible.

| Symbol | Location              | Meaning                                                                                                                                                             | Metadata to change                                                                 |
|--------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| `0`    | Number                | Digit                                                                                                                                                               |                                                                                    |
| `#`    | Number                | Digit, zero shows as absent                                                                                                                                         |                                                                                    |
| `.`    | Number                | Decimal separator or monetary decimal separator                                                                                                                     | [Decimal separator](#deciaml_separator) NB!<sup>[*](#pattern-parsing-star)</sup>   |
| `-`    | Number                | Minus sign                                                                                                                                                          | [Minus sign](#minus_sign) NB!<sup>[*](#pattern-parsing-star)</sup>                 |
| `,`    | Number                | Grouping separator                                                                                                                                                  | [Grouping separator](#grouping_separator) NB!<sup>[*](#pattern-parsing-star)</sup> |
| `E`    | Number                | Separates mantissa and exponent in scientific notation. Need not be quoted in prefix or suffix.                                                                     |                                                                                    |
| `;`    | Subpattern boundary   | Separates positive and negative subpatterns                                                                                                                         |                                                                                    |
| `%`    | Prefix or suffix      | Divide by 100 on parsing                                                                                                                                            |                                                                                    |
| `‰`    | Prefix or suffix      | Divide by 1000 on parsing                                                                                                                                           |                                                                                    |
| `'`    | Prefix or suffix      | Used to quote special characters in a prefix or suffix, for example, "'#'#" formats 123 to "#123". To create a single quote itself, use two in a row: "# o''clock". |                                                                                    |
| `∞`    | *not part of pattern* | String ti represent infinity                                                                                                                                        |                                                                                    |

<a name="pattern-parsing-star"/>\* while these can ne changed, in the [`pattern`]($pattern) the default value *must* be 
used. E.g. *"."*, *"."* and *"-"*.

**NB!** If there's no special format of the input, it's advised to avoid the usage of patterns due to the added
performance hit (parsing using a pattern adds computational overhead). It's still possible to redefine the special
symbols like [decimal separator](#decimal_separator) or [minus sign](#minus_sign) even without the pattern.

#### Number parsing peculiarities

- number of `#` and `0` in pattern is not reliable, stuff is parsed regardless (e.g. *"0.0"* produces the same result as
*"##.#*)
- Grouping signs placement "flexible"; in other words if grouping separator has been added to the pattern the number is
parser regardless of the position (e.g. pattern `#,##0.#` will parse _1,000.0_, _1,1234.5_ so as  _2000._)
- Grouping sign not accepted in decimal places
- _"+"_ not accepted if pattern is specified unless explicitly mentioned
- `%` and `‰` in fractional and decimal values and patterns divide the number by 100 and 1000 (in integrals as well, but
then they are usually not whole numbers)
- Even if redefined  the standard grouping and decimal separators **needs to be used** in the pattern
- If pattern is used, `e` is not accepted only `E` in the exponential expresion (without a pattern both are recognized)

Defaults
--------

As it was described, in case when the standardization is unsuccessful, either due to missing data in a column
non-nullable column or because the casting fails, default value is used in that particular column and row.

### Explicit default

The value optionally specified in the schema of the dataset for **Standardization** in _metadata_ [Default](#default)
property to be used for the particular column.

### Global default values

The value used in case of need of the default value and if _explicit default_ was not defined. The actual values are:

- `null` for nullable column
- `0` for numeric column
- `false` for Boolean column
- `""` (empty string) for string column
- `1970/01/01` for date column
- `1970/01/01 00:00:00` for timestamp column

- default timezone if not specified for both timestamps and dates is *UTC*; this can be changed via
- default locale and the decimal symbols based on that is *US*. Therefore:
  - minus sign is *"-"*
  - decimal separator is *"."*
  - grouping separator is *","*

### Explicit default values restrictions

- The value has to be a string convertible to the the field's type and fitting withing its size limitations (e.g._"200"_
cannot be a `default` for the type [`Short`](#short), or _"∞"_ if `allow_infinity` is _"false"_ for [`Double`](#double)/
[`Float`](#float))
- If it's a type supporting [`pattern`](#pattern) and it is defined, the default value has to adhere to the `pattern`
