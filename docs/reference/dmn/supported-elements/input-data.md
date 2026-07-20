---
sidebar_position: 4
---
# Input data

Input Data represents an external data source or variable that is provided as input to one or more decisions. It defines what information the decision requires from outside the DMN model.

## Key characteristics

- Not a decision itself — it has no logic.
- Connected to decisions via information requirements.
- Typically corresponds to a process variable passed into the DMN evaluation.

## Graphical notation

An oval shape labelled with the input data name.

<img src="/img/dmn/input-data.svg" alt="Input data" width="250" height="95" />

## XML Definition

```xml
<inputData id="customerType" name="Customer type" />
```
