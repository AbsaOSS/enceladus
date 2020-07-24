---
layout: docs
title: Usage - Menas Conformance Rules
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
---

## Table Of Contents
<!-- toc -->
- [Table Of Contents](#table-of-contents)
- [Intro](#intro)
- [Casting Conformance Rule](#casting-conformance-rule)
- [Concatenation Conformance Rule](#concatenation-conformance-rule)
- [Drop Conformance Rule](#drop-conformance-rule)
- [FillNulls Conformance Rule](#fillnulls-conformance-rule)
- [Literal Conformance Rule](#literal-conformance-rule)
- [Mapping Conformance Rule](#mapping-conformance-rule)
- [Negation Conformance Rule](#negation-conformance-rule)
- [SingleColumn Conformance Rule](#singlecolumn-conformance-rule)
- [SparkSessionConf Conformance Rule](#sparksessionconf-conformance-rule)
- [Uppercase Conformance Rule](#uppercase-conformance-rule)
<!-- tocstop -->

## Intro

Conformance Rules are a way for the user to enhance the data. Every conformance rule has an output column and an option for running a control measure. Output column defines the name of the output column into which the rule will output. Control measure check will run an Atum control measure check as in previous stages as defined in controls of `_INFO` file.

Every column also has one or more input columns.

**Important** - We never override a column. Each rule produces a new column.

## Casting Conformance Rule

Casting conformance rule Allows users to cast a specific column to another type. This conformance rule provides a selection of other types.

## Concatenation Conformance Rule

Concatenation conformance rule concatenates two or more input columns together into a single column. Columns are first transformed into their string representation before the actual concatenation.

## Drop Conformance Rule

Drop conformance rule removes a column from the output data.

## FillNulls Conformance Rule

FillNulls conformance rule takes a column and replaces all the nulls with a literal provided by the user.

## Literal Conformance Rule

Literal conformance rule adds a column with a string literal provided by the user.

## Mapping Conformance Rule

To use a mapping conformance rule, the user first needs to define a Mapping Table in Menas. Mapping Tables have the same properties and rules around them as Datasets, and it is expected of them to be in a parquet format.

When defining a mapping conformance rule, users first need to pick a correct Mapping Table and correct version. Then there is a question if the data can have Null values in join conditions. This means if the join that will be executed should be null safe or not.

Then the join conditions. A join condition conveys a relationship between the Dataset and MappingTable. The join condition specifies how the rows from one table will be combined with the rows of the other table. This is based on the equality of the rows.

Last is the target column, which specifies which column from the mapping table will be written into the output column.

## Negation Conformance Rule

Negation conformance rule negates any Numerical or Boolean value.

## SingleColumn Conformance Rule

Single column conformance rule transforms column into a column of structs of previous columns values. Input column alias here will be the name/key of the struct.

## SparkSessionConf Conformance Rule

SparkSessionConf conformance rule is able to pull out a configuration value from the SparkSession based on the key provided.

## Uppercase Conformance Rule

Uppercase conformance rule transforms all character letters in the column into capital letters.
