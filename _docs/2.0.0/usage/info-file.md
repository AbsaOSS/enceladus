---
layout: docs
title: Usage - _INFO file 
version: '2.0.0'
categories:
    - '2.0.0'
    - usage
redirect_from: /docs/usage/info-file
---
## Table Of Content

<!-- toc -->
- [Table Of Content](#table-of-content)
- [Description](#description)
- [Validation](#validation)
<!-- tocstop -->

## Description

File named `\_INFO` placed within the source directory together with the raw data is a JSON file tracking control measures 
via [Atum][atum]. Example how the file should contain can be found
[in the code][info-file].

## Validation

The _\_INFO file_ verification consists of checking that it has an array field of name `checkpoints`. The array has to
have at least two objects one named (field `name`) _"Raw"_ and one _"Source"_. Each of them has to have an array field
`controls`. This array has to contain a control of type count (`"controlType": "controlType.Count"`) with control value
(field `controlValue`) containing a positive integer.

E.g.

```json
{
  ...
  "checkpoints": [
    {
      "name": "Source",
      "processStartTime": "??? (timestamp)",
      "processEndTime": "??? (timestamp)",
      "workflowName": "Source",
      "order": "??? (positive integer)",
      "controls": [
        {
          "controlName": "recordCount",
          "controlType": "controlType.Count",
          "controlCol": "???",
          "controlValue": "??? (positive integer)"
        }
      ]
    },
    {
      "name": "Raw",
      "processStartTime": "??? (timestamp)",
      "processEndTime": "??? (timestamp)",
      "workflowName": "Raw",
      "order": "???",
      "controls": [
        {
          "controlName": "recordCount",
          "controlType": "controlType.Count",
          "controlCol": "???",
          "controlValue": "??? (positive integer)"
        }
      ]
    }
  ]
}
```

For a fully expanded example go [here][info-file].

[atum]: https://github.com/AbsaOSS/atum
[info-file]: https://github.com/AbsaOSS/enceladus/blob/master/examples/data/input/_INFO
