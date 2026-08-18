package bpmn20

// typedIndexBuilder accumulates the typed indexes for a TDefinitions
// during ResolveReferences. It walks the TProcess tree explicitly,
// using for i := range slice to obtain stable element pointers, and
// relies on a single register(node, ownerID) helper so that adding a
// new flow-node type cannot desynchronise the indexes.
type typedIndexBuilder struct {
	flowNodes        map[string]FlowNode
	internalTasks    map[string]InternalTask
	elementOwner     map[string]string
	subprocessParent map[string]string
}

// register inserts node into flowNodes, records its owning subprocess,
// and additionally registers it as an InternalTask if the underlying
// type implements that interface. Empty ids are ignored.
//
// Duplicate-id policy: if a duplicate non-empty id is encountered, the
// later node overwrites the earlier one in both flowNodes and
// elementOwner (last-wins). This is intentionally undefined behaviour
// for now — typed-walker level duplicate validation is deferred to a
// follow-up issue (see the plan's "Out of scope" section). The XML
// unmarshaler's ResolveReferences will still succeed; a future
// typed/schema-aware validator should reject duplicates before
// execution.
func (b *typedIndexBuilder) register(node FlowNode, ownerID string) {
	id := node.GetId()
	if id == "" {
		return
	}
	b.flowNodes[id] = node
	b.elementOwner[id] = ownerID
	if task, ok := node.(InternalTask); ok {
		b.internalTasks[id] = task
	}
}

// walkProcess walks the given process tree, registering every flow
// node it finds. ownerID is the id of the enclosing subprocess, or ""
// for the root process. Boundary events are intentionally skipped.
func (b *typedIndexBuilder) walkProcess(p *TProcess, ownerID string) {
	c := &p.TFlowElementsContainer
	for i := range c.StartEvents {
		b.register(&c.StartEvents[i], ownerID)
	}
	for i := range c.EndEvents {
		b.register(&c.EndEvents[i], ownerID)
	}
	for i := range c.ServiceTasks {
		b.register(&c.ServiceTasks[i], ownerID)
	}
	for i := range c.UserTasks {
		b.register(&c.UserTasks[i], ownerID)
	}
	for i := range c.BusinessRuleTask {
		b.register(&c.BusinessRuleTask[i], ownerID)
	}
	for i := range c.SendTask {
		b.register(&c.SendTask[i], ownerID)
	}
	for i := range c.ReceiveTask {
		b.register(&c.ReceiveTask[i], ownerID)
	}
	for i := range c.ParallelGateway {
		b.register(&c.ParallelGateway[i], ownerID)
	}
	for i := range c.ExclusiveGateway {
		b.register(&c.ExclusiveGateway[i], ownerID)
	}
	for i := range c.EventBasedGateway {
		b.register(&c.EventBasedGateway[i], ownerID)
	}
	for i := range c.InclusiveGateway {
		b.register(&c.InclusiveGateway[i], ownerID)
	}
	for i := range c.IntermediateCatchEvent {
		b.register(&c.IntermediateCatchEvent[i], ownerID)
	}
	for i := range c.IntermediateThrowEvent {
		b.register(&c.IntermediateThrowEvent[i], ownerID)
	}
	for i := range c.CallActivity {
		b.register(&c.CallActivity[i], ownerID)
	}
	// BoundaryEvent slice is intentionally not walked.
	for i := range c.SubProcess {
		sp := &c.SubProcess[i]
		b.register(sp, ownerID)
		b.subprocessParent[sp.GetId()] = ownerID
		b.walkProcess(&sp.TProcess, sp.GetId())
	}
}