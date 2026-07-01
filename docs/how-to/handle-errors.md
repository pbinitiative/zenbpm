# Handle Errors

ZenBPM supports BPMN error handling through **Error End Events** and **Error Boundary Events**. This allows you to model expected failure scenarios directly in the process diagram.

## How BPMN Error Handling Works

1. A **Job Worker** throws a BPMN error (using an error code) OR a **Script/Service Task** reaches an Error End Event.
2. The engine looks for a matching **Error Boundary Event** on the enclosing activity.
3. If found, the boundary event fires and the process continues along its outgoing flow.
4. If not found, the error propagates up the scope hierarchy. If still unhandled, it becomes an **incident**.

## Throw an Error from a Job Worker

```http
POST /jobs/{jobKey}/error
Content-Type: application/json

{
  "errorCode": "PAYMENT_DECLINED",
  "errorMessage": "Card declined by issuer"
}
```

The `errorCode` must match the `errorCode` attribute on the Error Boundary Event in the BPMN.

## BPMN Setup

### Error definition

```xml
<bpmn:error id="Error_PaymentDeclined"
            name="PaymentDeclined"
            errorCode="PAYMENT_DECLINED" />
```

### Error Boundary Event on the task

```xml
<bpmn:boundaryEvent id="OnPaymentDeclined"
                    name="Payment declined"
                    attachedToRef="Task_ProcessPayment">
  <bpmn:outgoing>Flow_ToRetry</bpmn:outgoing>
  <bpmn:errorEventDefinition errorRef="Error_PaymentDeclined" />
</bpmn:boundaryEvent>
```

### Error End Event (propagates up)

```xml
<bpmn:endEvent id="ErrorEnd" name="Payment failed">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:errorEventDefinition errorRef="Error_PaymentDeclined" />
</bpmn:endEvent>
```

## Common Patterns

### Retry with escalation

Use a counter variable to limit retries, then escalate to a manual task after N failures:

```
Service Task → [Error Boundary] → increment retry counter
                                → [Exclusive Gateway] → retry < 3 → loop back
                                                      → retry >= 3 → Manual review task
```

### Compensation / fallback path

Route errors to an alternative flow that compensates for the failed step (e.g. send a failure notification, refund a payment).

## Incidents

If an error is not caught by any boundary event, ZenBPM creates an **incident** on the process instance. Incidents pause the execution and require manual intervention — either resolving the root cause and retrying, or cancelling the instance.

## Next Steps

- [Use process variables](./use-process-variables) to store error details for auditing
- [Implement a Job Worker](./implement-job-worker) that throws BPMN errors
