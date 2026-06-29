# How to Implement a Job Worker

A Job Worker handles the execution of **Service Tasks** in a BPMN process. When the BPMN engine reaches a Service Task, it creates a job and waits for a worker to pick it up, execute the business logic, and report the result.

## How Job Workers Work

1. The engine creates a job when it reaches a Service Task.
2. Your worker polls for available jobs (or uses streaming).
3. The worker processes the job and calls **Complete** or **Fail**.
4. The engine continues (or handles the failure).

## Fetch and Lock a Job

Fetch available jobs for a specific job type:

```http
POST /jobs/activate
Content-Type: application/json

{
  "jobType": "payment-service",
  "maxJobsToActivate": 10,
  "workerName": "payment-worker-1",
  "lockDurationMs": 30000
}
```

| Field | Description |
|---|---|
| `jobType` | Must match the `type` attribute on the Service Task in the BPMN |
| `maxJobsToActivate` | How many jobs to fetch in one call |
| `lockDurationMs` | How long the job is locked for this worker (in milliseconds) |

The response returns a list of activated jobs, each containing:
- `jobKey` — unique identifier of the job
- `variables` — process variables available to the worker
- `processInstanceKey` — the instance this job belongs to

## Complete a Job

After successfully processing the job:

```http
POST /jobs/{jobKey}/complete
Content-Type: application/json

{
  "variables": {
    "paymentStatus": "approved",
    "transactionId": "TXN-98765"
  }
}
```

Variables returned in the complete call are written back to the process instance.

## Fail a Job

If the job cannot be completed (e.g. external service unavailable):

```http
POST /jobs/{jobKey}/fail
Content-Type: application/json

{
  "errorMessage": "Payment service timeout",
  "retries": 2
}
```

If `retries` reaches 0, the job becomes an **incident** that requires manual intervention.

## Throw a BPMN Error

To trigger an Error Boundary Event on the parent activity:

```http
POST /jobs/{jobKey}/error
Content-Type: application/json

{
  "errorCode": "PAYMENT_DECLINED",
  "errorMessage": "Payment was declined by the provider"
}
```

The `errorCode` must match the `errorCode` attribute on the Error Boundary Event in the BPMN diagram.

## Example: Go Worker

```go
func processPaymentJob(ctx context.Context, job client.Job) {
    variables, _ := job.GetVariablesAsMap()
    amount := variables["orderAmount"].(float64)

    // Business logic
    txnID, err := paymentService.Charge(amount)
    if err != nil {
        job.Fail(ctx, client.FailJobRequest{
            ErrorMessage: err.Error(),
            Retries:      job.Retries - 1,
        })
        return
    }

    job.Complete(ctx, client.CompleteJobRequest{
        Variables: map[string]interface{}{
            "transactionId": txnID,
            "paymentStatus": "approved",
        },
    })
}
```

## Best Practices

- Always set a realistic `lockDurationMs` — if your worker takes longer than the lock duration, another worker may pick up the same job.
- Use `retries` to distinguish transient errors (retry) from permanent failures (throw BPMN error).
- Keep worker logic idempotent — the same job may be delivered more than once in edge cases.

## Next Steps

- [Send a message](./send-receive-messages) to correlate external events with the process
- [Handle errors](./handle-errors) with Error Boundary Events
