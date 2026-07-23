---
sidebar_position: 30
---

# Error events

Error events model **business failures** that divert the process to an explicit error path — a declined payment, an item out of stock. A BPMN error is identified by an error code: it is *thrown* by an error end event or by a failing job worker, and *caught* by an error boundary event — or the error start event of an [event sub process](../activities/event-sub-process.md) — with a matching code on an enclosing scope. Technical failures without a matching catch become incidents instead.

<table className="bpmn-types-table">
  <thead>
    <tr>
      <th>Position</th>
      <th style={{width: '110px'}}>Icon</th>
      <th>Behavior</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="#error-boundary-event">Error boundary event</a></td>
      <td><a href="#error-boundary-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/error-boundary-interrupting.svg').default} alt="Error boundary event" height="70" /></a></td>
      <td>Catches a matching error raised inside the attached activity. Always interrupting.</td>
    </tr>
    <tr>
      <td><a href="#error-end-event">Error end event</a></td>
      <td><a href="#error-end-event"><img className="bpmn-supported" src={require('!url-loader!../../../assets/bpmn/events/error-end.svg').default} alt="Error end event" height="70" /></a></td>
      <td>Ends the path by throwing an error to an enclosing scope.</td>
    </tr>
  </tbody>
</table>

## Use cases

- **Handle business errors from workers** — a payment worker reports `PAYMENT_DECLINED` instead of completing the job; an error boundary event on the service task routes the order to a retry-payment path.
- **Signal failure out of a subprocess** — an error end event inside a sub process or called process ends it as failed, and the boundary event on the enclosing activity decides what happens next.
- **Separate the happy path from failure paths** — model each business failure as a named error with its own outgoing flow instead of encoding failure flags in variables and gateways.

## Usage in BPMN

Errors are declared once as `bpmn:error` elements and referenced by throwing and catching events. Matching is done by the **error code**.

| Markup                       | Attribute   | Required                     | Description                                                                                                                                    |
| ---------------------------- | ----------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `bpmn:error`                 | `errorCode` | yes                          | Identifies the error. Thrown and caught errors are matched by this code.                                                                       |
| `bpmn:errorEventDefinition`  | `errorRef`  | end event: yes; boundary event: no | References the `bpmn:error`. A boundary event **without** `errorRef` is a catch-all that catches any error reaching its activity.              |

How an error travels through the process:

1. An error is raised — a token reaches an error end event, or a job worker reports a business error with an error code instead of completing the job.
2. The engine looks for a matching error handler, starting at the enclosing activity and walking up the scope hierarchy — surrounding sub processes, then the call activities of parent processes. At each scope, both error boundary events and error [event sub processes](../activities/event-sub-process.md) are considered: a boundary event attached to the throwing activity wins over the scope's event sub process, and a handler referencing a specific error code wins over a catch-all.
3. The first match wins: every scope between the throw and the catch is terminated, the boundary event interrupts its activity, and the token continues along the boundary event's outgoing flow. When the error comes from a failed job, the job's variables form the payload of the catching event — all of them are propagated unless `zenbpm:output` mappings select specific ones (see [Variables](../../variable-mapping.md)).
4. If no scope catches the error, an **incident** is recorded and the affected part of the process stops for manual intervention.

### Error boundary event

Catches errors raised inside the activity it is attached to — including errors thrown arbitrarily deep inside a sub process or called process. It is **always interrupting**: the host activity (and any scopes the error travelled through) is cancelled before the token continues on the error path. With `errorRef` it catches only the referenced error code; without it, it catches every error.

### Error end event

Ends the current path by throwing the referenced error instead of completing normally. The scope containing the event is terminated — including its other active tokens — and the error propagates up the hierarchy as described above. An error end event in the top-level process has no enclosing scope to catch the error, so it always results in an incident.

## Related documentation

- [Variables](../../variable-mapping.md) — payload propagation rules for catching events.
- [Service task](../activities/tasks/service-task.md) — the job-based task whose failures error boundary events typically handle.
- [Sub process](../activities/sub-process.md) and [Call activity](../activities/call-activity.md) — the scopes errors propagate out of.
- [Event sub process](../activities/event-sub-process.md) — catching errors at scope level through an error start event.

## XML example

A payment sub process throws `PAYMENT_DECLINED` via an error end event; the boundary event on the sub process catches it and routes to a recovery path:

```xml
<bpmn:error id="Error_PaymentDeclined" name="Payment declined" errorCode="PAYMENT_DECLINED" />

<bpmn:subProcess id="Activity_TakePayment" name="Take payment">
  <!-- ... payment steps ... -->
  <bpmn:endEvent id="End_Declined" name="Payment declined">
    <bpmn:incoming>Flow_Declined</bpmn:incoming>
    <bpmn:errorEventDefinition errorRef="Error_PaymentDeclined" />
  </bpmn:endEvent>
</bpmn:subProcess>

<bpmn:boundaryEvent id="Boundary_PaymentDeclined" name="On declined"
                    attachedToRef="Activity_TakePayment">
  <bpmn:outgoing>Flow_OfferOtherPayment</bpmn:outgoing>
  <bpmn:errorEventDefinition errorRef="Error_PaymentDeclined" />
</bpmn:boundaryEvent>
```
