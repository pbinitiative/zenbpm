---
sidebar_position: 70
---
# Inclusive Gateway

An Inclusive (OR) gateway activates one or more outgoing paths whose conditions are true, allowing multiple branches to run simultaneously.

## Key characteristics

- **Diverging:** activates every outgoing path whose condition evaluates to true.
- A **default flow** is taken only if no other condition matches.
- **Converging:** waits for all active incoming branches before continuing.

## Graphical notation

A diamond with a circle ("O") icon.

<img src={require('!url-loader!../../../assets/bpmn/gateways/inclusive-gateway.svg').default} alt="Inclusive gateway usage example" width="120" height="120" />

## XML Definition

```xml
<bpmn:inclusiveGateway id="chooseNotifications" name="Which notifications?" default="Flow_default">
  <bpmn:incoming>Flow_1</bpmn:incoming>
  <bpmn:outgoing>Flow_email</bpmn:outgoing>
  <bpmn:outgoing>Flow_sms</bpmn:outgoing>
  <bpmn:outgoing>Flow_default</bpmn:outgoing>
</bpmn:inclusiveGateway>

<bpmn:sequenceFlow id="Flow_email" sourceRef="chooseNotifications" targetRef="Task_SendEmail">
  <bpmn:conditionExpression>= customer.emailOptIn = true</bpmn:conditionExpression>
</bpmn:sequenceFlow>
<bpmn:sequenceFlow id="Flow_sms" sourceRef="chooseNotifications" targetRef="Task_SendSms">
  <bpmn:conditionExpression>= customer.smsOptIn = true</bpmn:conditionExpression>
</bpmn:sequenceFlow>
<bpmn:sequenceFlow id="Flow_default" sourceRef="chooseNotifications" targetRef="Task_SendPushNotification" />
```

## Practical example

A notification process sends an email, an SMS, or both depending on the customer's communication preferences. If neither condition matches, a push notification is sent as the default. The Inclusive Gateway can activate multiple paths at once.

## Current Implementation

Fully supported, including default flows and FEEL condition expressions.
