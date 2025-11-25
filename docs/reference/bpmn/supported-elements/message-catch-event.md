---
sidebar_position: 100
---

A Message Catch Event is used to model the recipient of a message from an external participant or process.
It indicates that the execution flow will pause until the specified message is received.
**ZenBPM implementation** of the engine tries to provide guarantees that only one message is active in the system with the same name and correlation key.

**Uniqueness**
Message Catch Event is identified by a **Correlation Key** and **Correlation Name**.
Combination of the two parameters is unique for each message catch event.
Running into Message Catch Event with an already existing combination results in Incident(link).

#### Message subscription

Message subscription is what we refer to when the engine stores data about the Message Catch Event.

#### Message subscription pointer

Message subscription pointers are used to correlate messages in Zen's architecture of a [distributed system](/reference/cluster).
Pointers are used to efficiently determine location of a stored subscription, without requiring any internal identifiers.
Pointer's storage partition is calculated from user defined **Correlation Key**.
From which we can determine partition assigned to **Message Subscription** and its **Process Instance** to then correlate the message.

```mermaid
---
config:
  layout: elk
---
flowchart LR
 subgraph s2["Node 2 (partition 1 follower)"]
        n1["Node<br>step 2: get partition id of the message pointer from correlation key<br>step 4: find leader for message that message pointer points to"]
  end
 subgraph s3["Node 1 (partition 1 leader)"]
        n2["Node<br>"]
        n5["BPMN engine"]
  end
 subgraph s4["Node 3 (partition 2 leader)"]
        n3["Node<br> stores message pointer based on correlation key hash"]
  end
 subgraph s1["Correlate message in a 2 partition setup"]
        n0["public API"]
        s2
        s3
        s4
  end
    n0 L_n0_n1_0@-- step:1 Publish message --> n1
    n1 L_n1_n2_0@-- step:5 publish message internally --> n2
    n1 L_n1_n3_0@-- step:3 load message pointer --> n3
    n2 L_n2_n5_0@-- step:6 activate the message<br> in the engine --> n5
    n5 L_n5_n3_0@-- step:7 update message pointer state --> n3
     n0:::Previous
    classDef Hidden stroke-width:1px, stroke-dasharray:none, opacity: 10%
    classDef Previous stroke-width:1px, stroke-dasharray:none, opacity: 40%
    L_n1_n2_0@{ animation: fast }
    L_n1_n3_0@{ animation: fast }
    L_n0_n1_0@{ animation: fast }
    L_n2_n5_0@{ animation: fast }
    L_n5_n3_0@{ animation: fast }
```

#### Usage

Messages can be published through API or by triggering [Intermediate throw event](/reference/bpmn-engine/supported-elements/message-throw-event).
<!-- <ApiOperation id="api" pointer="#/paths/~1messages/post" example={true} />-->

:::note[Future plans]
We plan to implement message start event along with timer start events in the future
:::