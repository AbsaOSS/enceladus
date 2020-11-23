---
layout: docs
title: Usage - Menas API
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
---

- [Export Entity](#export-entity)
  - [Example Import Response for Schema](#example-import-response-for-schema)
- [Entity Import](#entity-import)
  - [JSON payload](#json-payload)
  - [Example Export Response for Schema](#example-export-response-for-schema)


### Export Entity

This endpoint exports a single entity from Menas. All exports are versionless, except for connected entities. They are kept as is, but need to be validated before import.

`GET {menas_url}/api/{entity}/exportItem/{name}/{version}`

- entity is `schema`, `dataset` or `mappingTable`
- name is the name of the entity as seen in Menas
- version is the version of the entity as seen in Menas. Version is optional and in that case the latest version will be used

#### Example Import Response for Schema

Returns code is 200.

**Top level keys**

| Key | Value | Description |
|---|---|---|
| metadata | Struct | Metadata for the export functionality |
| item | Struct | exported entity |

**Metadata keys**

| Key | Value | Description |
|---|---|---|
| exportVersion | Integer | Version of the export and model used |

**Item keys**

| Key | Value | Description |
|---|---|---|
| name | String | the name of the entity |
| description | String | the description of the entity |
| fields | Struct | fields specified in the schema. A complex type representing columns. |

**Fields keys**

| Key | Value | Description |
|---|---|---|
| name | String | the name of the column |
| type | String | the type of the column |
| path | String | parent column name |
| absolutePath | String | absolute path to the column |
| elementType | String | if field is a struct or array |
| nullable | Boolean | boolean value specifying nullability |
| metadata | Struct | metadata of the field, like patters, default value, etc. |
| children | Array of Fields | array of fields. Sub-columns/fields to this one. |


Example:
```json
{
  "metadata": {
    "exportVersion": 1
  },
  "item": {
    "name": "Name",
    "description": "",
    "fields": [
      {
        "name": "Boolean",
        "type": "boolean",
        "path": "",
        "elementType": null,
        "containsNull": null,
        "nullable": true,
        "metadata": {
          "default": null
        },
        "children": [],
        "absolutePath": "Boolean"
      },
      {
        "name": "Byte",
        "type": "byte",
        "path": "",
        "elementType": null,
        "containsNull": null,
        "nullable": true,
        "metadata": {},
        "children": [],
        "absolutePath": "Byte"
      }
    ]
  }
}
```

### Entity Import

This endpoint imports a single entity. All imports are versionless. If the import does not find an entity with the same name it will create a new one and start the version from 1. If the import finds an existing version, it will update the previous version.

Versions of connected entities need to be specified properly. Export of a Dataset carries a Schema and maybe some Mapping tables as connected entities. These have versions and these versions need to exist on Import.

`POST {menas_url}//api/{entity}/importItem`

- entity is `schema`, `dataset` or `mappingTable`
- expects a JSON payload

#### JSON payload

JSON payload is the same as the JSON response from the [export](#example-import-response-for-schema).

#### Example Export Response for Schema

On success, it is the same as JSON payload of update or create API, depending if the entity name already existed or not. Response code is 201.

On failure, you will get a list of errors produced by the validation like bellow

```json
{
  "errors": {
    "item.name": [
      "name 'null' contains unsupported characters"
    ],
    "metadata.exportApiVersion": [
      "Export/Import API version mismatch. Acceptable version is 1. Version passed is 2"
    ]
  }
}
```

You have a key `errors`, which is a struct that will hold other keys from the JSON Payload sent and messages of the issues found with this key. 
There can be multiple messages connected to one key.

In this example we see that we have forgoten to send `name` of the entity and there was a mismatch between versions of export/import used. 
