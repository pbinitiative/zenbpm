# How to Use Process Variables

Process variables store data within a process instance. They are available to all elements in the process scope and can be read or updated at any point during execution.

## Variable Scopes

Variables in ZenBPM are scoped:

| Scope | Description |
|---|---|
| **Process instance** | Available to all elements in the process |
| **Sub-process** | Local to the sub-process; merged into parent on completion |
| **Multi-instance** | Each instance has its own copy of the element variable |

## Set Variables When Starting an Instance

Pass initial variables in the start request:

```http
POST /process-instances
Content-Type: application/json

{
  "processDefinitionKey": "order-process",
  "variables": {
    "orderId": "ORD-001",
    "amount": 1500,
    "currency": "CZK"
  }
}
```

## Read Variables from a Running Instance

```http
GET /process-instances/{processInstanceKey}/variables
```

Returns all variables currently set on the instance.

## Update Variables on a Running Instance

```http
PUT /process-instances/{processInstanceKey}/variables
Content-Type: application/json

{
  "approvalStatus": "approved",
  "approvedBy": "martin@example.com",
  "approvedAt": "2026-06-29T10:00:00Z"
}
```

Variables are merged — existing variables not included in the request are not changed.

## Variables in Job Workers

When a worker completes a job, it can write variables back to the instance:

```http
POST /jobs/{jobKey}/complete
Content-Type: application/json

{
  "variables": {
    "invoiceId": "INV-555",
    "invoiceUrl": "https://example.com/invoices/555.pdf"
  }
}
```

## Variable Types

ZenBPM supports standard JSON types:

| Type | Example |
|---|---|
| String | `"status": "pending"` |
| Number | `"amount": 1500` |
| Boolean | `"approved": true` |
| Object | `"address": { "city": "Prague" }` |
| Array | `"items": ["A", "B", "C"]` |
| Null | `"cancelledAt": null` |

## FEEL Expressions

Variables can be referenced in FEEL expressions (e.g. on Exclusive Gateway conditions, DMN rules):

```
= amount > 1000 and currency = "CZK"
```

See the [Script Engine reference](/reference/script-engine) for supported FEEL syntax.

## Best Practices

- Use **descriptive variable names** in camelCase (e.g. `customerEmail`, `orderTotal`).
- Avoid storing large binary data as variables — store a reference (URL, ID) instead.
- Clean up temporary variables when they are no longer needed to keep the instance lean.

## Next Steps

- [Start a process instance](./start-process-instance) with initial variables
- [Implement a Job Worker](./implement-job-worker) that reads and writes variables
