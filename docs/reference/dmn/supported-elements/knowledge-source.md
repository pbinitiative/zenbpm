---
sidebar_position: 5
---
# Knowledge source

A Knowledge Source is a reference to the authority, regulation, or document that justifies or governs a decision. It is a documentation artifact with no effect on execution.

## Key characteristics

- Has no impact on decision evaluation.
- Used purely for documentation and traceability.
- Linked to decisions or business knowledge models via authority requirements.

## Graphical notation

A document-like shape (truncated rectangle) labelled with the source name.

<img src={require('!url-loader!../../assets/dmn/knowledge-source.svg').default} alt="Knowledge source" width="250" height="175" />

## XML Definition

```xml
<knowledgeSource id="taxRegulation" name="Tax regulation 2024" />
```
