package bpmn

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"log"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

const CurrentSerializerVersion = 1

type serializedBpmnEngine struct {
	Version              int
	Name                 string
	ProcessReferences    []processInfoReference
	ProcessInstances     []*processInstanceInfo
	MessageSubscriptions []*runtime.MessageSubscription
	Timers               []*runtime.Timer
	Jobs                 []*runtime.Job
}

type processInfoReference struct {
	BpmnProcessId    string // The ID as defined in the BPMN file
	ProcessKey       int64  // The engines key for this given process with version
	BpmnData         string // the raw BPMN XML data
	BpmnResourceName string // the resource's name
	BpmnChecksum     string // internal checksum to identify different versions
}

type ProcessInstanceInfoAlias processInstanceInfo // FIXME: don't export
type processInstanceInfoAdapter struct {
	ProcessKey       int64
	ActivityAdapters []*activityAdapter
	*ProcessInstanceInfoAlias
}

type timerAlias runtime.Timer
type timerAdapter struct {
	OriginActivitySurrogate activitySurrogate
	*timerAlias
}

type messageSubscriptionAlias runtime.MessageSubscription
type messageSubscriptionAdapter struct {
	OriginActivitySurrogate activitySurrogate
	*messageSubscriptionAlias
}

type activityAdapterType int

const (
	gatewayActivityAdapterType = iota
	eventBasedGatewayActivityAdapterType
)

type activityAdapter struct {
	Type                      activityAdapterType
	Key                       int64
	State                     runtime.ActivityState
	ElementReference          string
	Parallel                  bool     // from gatewayActivity
	InboundFlowIdsCompleted   []string // from gatewayActivity
	OutboundActivityCompleted string   // from eventBasedGatewayActivity
}

// activitySurrogate only exists to have a simple way of marshalling originActivities in MessageSubscription and Timer
// Deprecated: should be replaced by storage.PersistentStorageNew
// TODO see issue https://github.com/pbinitiative/zenbpm/issues/190
type activitySurrogate struct {
	ActivityKey        int64
	ActivityState      runtime.ActivityState
	ElementReferenceId string
	elementReference   bpmn20.FlowNode
}

// Deprecated: should be replaced by storage.PersistentStorageNew
type baseElementPlaceholder struct {
	id string
}

func (b baseElementPlaceholder) GetId() string {
	return b.id
}
func (b baseElementPlaceholder) GetDocumentation() []bpmn20.Documentation {
	panic("the placeholder does not implement all methods, by intent")
}

func (b baseElementPlaceholder) GetName() string {
	panic("the placeholder does not implement all methods, by intent")
}

func (b baseElementPlaceholder) GetIncomingAssociation() []string {
	panic("the placeholder does not implement all methods, by intent")
}

func (b baseElementPlaceholder) GetOutgoingAssociation() []string {
	panic("the placeholder does not implement all methods, by intent")
}

func (b baseElementPlaceholder) GetType() bpmn20.ElementType {
	panic("the placeholder does not implement all methods, by intent")
}

// ----------------------------------------------------------------------------

// Deprecated: should be replaced by storage.PersistentStorageNew
type activityPlaceholder struct {
	key int64
}

func (a activityPlaceholder) Key() int64 {
	return a.key
}

func (a activityPlaceholder) State() runtime.ActivityState {
	panic("the placeholder does not implement all methods, by intent")
}

func (a activityPlaceholder) Element() bpmn20.FlowNode {
	panic("the placeholder does not implement all methods, by intent")
}

// ----------------------------------------------------------------------------

//func (t *runtime.Timer) MarshalJSON() ([]byte, error) {
//	ta := &timerAdapter{
//		timerAlias: (*timerAlias)(t),
//	}
//	// TODO see issue https://github.com/pbinitiative/zenbpm/issues/190
//	ta.OriginActivitySurrogate = activitySurrogate{
//		ActivityKey:        t.OriginActivity.Key(),
//		ActivityState:      t.OriginActivity.State(),
//		ElementReferenceId: t.OriginActivity.Element().GetId(),
//	}
//	return json.Marshal(ta)
//}
//
//func (t *runtime.Timer) UnmarshalJSON(data []byte) error {
//	ta := timerAdapter{
//		timerAlias: (*timerAlias)(t),
//	}
//	if err := json.Unmarshal(data, &ta); err != nil {
//		return err
//	}
//	t.OriginActivity = ta.OriginActivitySurrogate
//	return nil
//}

// ----------------------------------------------------------------------------

//func (m *runtime.MessageSubscription) MarshalJSON() ([]byte, error) {
//	msa := &messageSubscriptionAdapter{
//		messageSubscriptionAlias: (*messageSubscriptionAlias)(m),
//	}
//	// TODO see issue https://github.com/pbinitiative/zenbpm/issues/190
//	msa.OriginActivitySurrogate = activitySurrogate{
//		ActivityKey:        m.OriginActivity.Key(),
//		ActivityState:      m.OriginActivity.State(),
//		ElementReferenceId: m.OriginActivity.Element().GetId(),
//	}
//	return json.Marshal(msa)
//}
//
//func (m *runtime.MessageSubscription) UnmarshalJSON(data []byte) error {
//	msa := messageSubscriptionAdapter{
//		messageSubscriptionAlias: (*messageSubscriptionAlias)(m),
//	}
//	if err := json.Unmarshal(data, &msa); err != nil {
//		return err
//	}
//	m.OriginActivity = msa.OriginActivitySurrogate
//	return nil
//}

// ----------------------------------------------------------------------------

// Deprecated: should be replaced by storage.PersistentStorageNew
func (pii *processInstanceInfo) MarshalJSON() ([]byte, error) {
	piia := &processInstanceInfoAdapter{
		ProcessKey:               pii.ProcessInfo.ProcessKey,
		ProcessInstanceInfoAlias: (*ProcessInstanceInfoAlias)(pii),
	}
	for _, a := range pii.activities {
		switch activity := a.(type) {
		case *gatewayActivity:
			piia.ActivityAdapters = append(piia.ActivityAdapters, createGatewayActivityAdapter(activity))
		case *eventBasedGatewayActivity:
			piia.ActivityAdapters = append(piia.ActivityAdapters, createEventBasedGatewayActivityAdapter(activity))
		default:
			panic(fmt.Sprintf("[invariant check] missing activity adapter for the type %T", a))
		}
	}
	return json.Marshal(piia)
}

// Deprecated: should be replaced by storage.PersistentStorageNew
func (pii *processInstanceInfo) UnmarshalJSON(data []byte) error {
	adapter := &processInstanceInfoAdapter{
		ProcessInstanceInfoAlias: (*ProcessInstanceInfoAlias)(pii),
	}
	if err := json.Unmarshal(data, &adapter); err != nil {
		return err
	}
	pii.ProcessInfo = &runtime.ProcessDefinition{ProcessKey: adapter.ProcessKey}
	recoverProcessInstanceActivitiesPart1(pii, adapter.ActivityAdapters)
	return nil
}

// Deprecated: should be replaced by storage.PersistentStorageNew
func createEventBasedGatewayActivityAdapter(ebga *eventBasedGatewayActivity) *activityAdapter {
	aa := &activityAdapter{
		Type:                      eventBasedGatewayActivityAdapterType,
		Key:                       ebga.key,
		State:                     ebga.state,
		ElementReference:          ebga.element.GetId(),
		OutboundActivityCompleted: ebga.OutboundActivityCompleted,
	}
	return aa
}

// Deprecated: should be replaced by storage.PersistentStorageNew
func createGatewayActivityAdapter(ga *gatewayActivity) *activityAdapter {
	aa := &activityAdapter{
		Type:                    gatewayActivityAdapterType,
		Key:                     ga.key,
		State:                   ga.state,
		ElementReference:        ga.element.GetId(),
		Parallel:                ga.parallel,
		InboundFlowIdsCompleted: ga.inboundFlowIdsCompleted,
	}
	return aa
}

// ----------------------------------------------------------------------------

func (a activitySurrogate) Key() int64 {
	return a.ActivityKey
}

func (a activitySurrogate) State() runtime.ActivityState {
	return a.ActivityState
}

func (a activitySurrogate) Element() bpmn20.FlowNode {
	return a.elementReference
}

// ----------------------------------------------------------------------------

// Deprecated: should be replaced by storage.PersistentStorageNew
func (state *Engine) Marshal() []byte {
	m := serializedBpmnEngine{
		Version:              CurrentSerializerVersion,
		Name:                 state.name,
		MessageSubscriptions: state.persistence.FindMessageSubscription(nil, nil, nil),
		ProcessReferences:    createReferences(state.persistence.FindProcessesById("")),
		ProcessInstances:     state.persistence.FindProcessInstances(-1),
		Timers:               state.persistence.FindTimers(nil, nil),
		Jobs:                 state.persistence.FindJobs(nil, nil, nil, nil),
	}
	bytes, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return bytes
}

// Unmarshal loads the data byte array and creates a new instance of the BPMN Engine
// Will return an BpmnEngineUnmarshallingError, if there was an issue AND in case of error,
// the engine return object is only partially initialized and likely not usable
// Deprecated: should be replaced by storage.PersistentStorageNew
func Unmarshal(data []byte) (Engine, error) {
	eng := serializedBpmnEngine{}
	err := json.Unmarshal(data, &eng)
	if err != nil {
		panic(err)
	}
	// TODO: why is unmarshal creating new bpmn engine instance?
	// state := New()
	// state.name = eng.Name
	// if eng.ProcessReferences != nil {
	// 	for _, pir := range eng.ProcessReferences {
	// 		xmlData, err := decodeAndDecompress(pir.BpmnData)
	// 		if err != nil {
	// 			msg := "Can't decode nor decompress serialized BPMN data"
	// 			return state, &BpmnEngineUnmarshallingError{
	// 				Msg: msg,
	// 				Err: err,
	// 			}
	// 		}
	// 		process, err := state.load(xmlData, pir.BpmnResourceName)
	// 		if err != nil {
	// 			msg := "Can't load BPMN from serialized data"
	// 			return state, &BpmnEngineUnmarshallingError{
	// 				Msg: msg,
	// 				Err: err,
	// 			}
	// 		}
	// 		process.ProcessKey = pir.ProcessKey
	// 	}
	// }
	// if eng.ProcessInstances != nil {
	// 	state.processInstances = eng.ProcessInstances
	// 	err := recoverProcessInstances(&state)
	// 	if err != nil {
	// 		return state, err
	// 	}
	// }
	// recoverProcessInstanceActivitiesPart2(&state)
	// if eng.MessageSubscriptions != nil {
	// 	state.messageSubscriptions = eng.MessageSubscriptions
	// 	err = recoverMessageSubscriptions(&state)
	// 	if err != nil {
	// 		return state, err
	// 	}
	// }
	// if eng.Timers != nil {
	// 	state.timers = eng.Timers
	// 	err = recoverTimers(&state)
	// 	if err != nil {
	// 		return state, err
	// 	}
	// }
	// if eng.Jobs != nil {
	// 	state.jobs = eng.Jobs
	// 	err = recoverJobs(&state)
	// 	if err != nil {
	// 		return state, err
	// 	}
	// }
	return Engine{}, nil
}

func recoverProcessInstanceActivitiesPart1(pii *processInstanceInfo, activityAdapters []*activityAdapter) {
	for _, aa := range activityAdapters {
		switch aa.Type {
		case gatewayActivityAdapterType:
			var elementPlaceholder bpmn20.FlowNode = &baseElementPlaceholder{id: aa.ElementReference}
			pii.activities = append(pii.activities, &gatewayActivity{
				key:                     aa.Key,
				state:                   aa.State,
				element:                 elementPlaceholder,
				parallel:                aa.Parallel,
				inboundFlowIdsCompleted: aa.InboundFlowIdsCompleted,
			})
		case eventBasedGatewayActivityAdapterType:
			var elementPlaceholder bpmn20.FlowNode = baseElementPlaceholder{id: aa.ElementReference}
			pii.activities = append(pii.activities, &eventBasedGatewayActivity{
				key:                       aa.Key,
				state:                     aa.State,
				element:                   elementPlaceholder,
				OutboundActivityCompleted: aa.OutboundActivityCompleted,
			})
		default:
			panic(fmt.Sprintf("[invariant check] missing recovery code for actictyAdapter.Type=%d", aa.Type))
		}
	}
}

func recoverProcessInstanceActivitiesPartWithBaseElements(pii *processInstanceInfo, activityAdapters []*activityAdapter) {
	for _, aa := range activityAdapters {
		bes := bpmn20.FindFlowNodesById(&pii.ProcessInfo.Definitions, aa.ElementReference)
		if len(bes) == 0 {
			log.Printf("Could not find base element with id %s", aa.ElementReference)
			continue
		}
		switch aa.Type {
		case gatewayActivityAdapterType:
			pii.activities = append(pii.activities, &gatewayActivity{
				key:                     aa.Key,
				state:                   aa.State,
				element:                 bes[0],
				parallel:                aa.Parallel,
				inboundFlowIdsCompleted: aa.InboundFlowIdsCompleted,
			})
		case eventBasedGatewayActivityAdapterType:
			pii.activities = append(pii.activities, &eventBasedGatewayActivity{
				key:                       aa.Key,
				state:                     aa.State,
				element:                   bes[0],
				OutboundActivityCompleted: aa.OutboundActivityCompleted,
			})
		default:
			panic(fmt.Sprintf("[invariant check] missing recovery code for actictyAdapter.Type=%d", aa.Type))
		}
	}
}

func recoverProcessInstanceActivitiesPart2(state *Engine) {
	// for _, pi := range state.processInstances {
	// 	for _, a := range pi.activities {
	// 		switch activity := a.(type) {
	// 		case *eventBasedGatewayActivity:
	// 			activity.element = BPMN20.FindFlowNodesById(&pi.ProcessInfo.definitions, (*a.Element()).GetId())[0]
	// 		case *gatewayActivity:
	// 			activity.element = BPMN20.FindFlowNodesById(&pi.ProcessInfo.definitions, (*a.Element()).GetId())[0]
	// 		default:
	// 			panic(fmt.Sprintf("[invariant check] missing case for activity type=%T", a))
	// 		}
	// 	}
	// }
}

// ----------------------------------------------------------------------------

func recoverProcessInstances(state *Engine) error {
	// for i, pi := range state.processInstances {
	// 	process := state.findProcess(pi.ProcessInfo.ProcessKey)
	// 	if process == nil {
	// 		msg := fmt.Sprintf("Can't find process key %d in current BPMN Engine's processes", pi.ProcessInfo.ProcessKey)
	// 		return &BpmnEngineUnmarshallingError{
	// 			Msg: msg,
	// 		}
	// 	}
	// 	state.processInstances[i].ProcessInfo = process
	// 	state.processInstances[i].VariableHolder = var_holder.New(nil, nil)
	// }
	return nil
}

func recoverJobs(state *Engine) error {
	// for _, j := range state.jobs {
	// 	pi := state.FindProcessInstance(j.ProcessInstanceKey)
	// 	if pi == nil {
	// 		return &BpmnEngineUnmarshallingError{
	// 			Msg: fmt.Sprintf("can't find process instannce with key %d; "+
	// 				"the marshalled JSON was likely corrupt", j.ProcessInstanceKey),
	// 		}
	// 	}
	// 	definitions := pi.ProcessInfo.definitions
	// 	activity := BPMN20.FindFlowNodesById(&definitions, j.ElementId)[0]
	// 	j.baseElement = activity
	// }
	return nil
}

func recoverTimers(state *Engine) error {
	// for _, t := range state.timers {
	// 	pi := state.FindProcessInstance(t.ProcessInstanceKey)
	// 	if pi == nil {
	// 		return &BpmnEngineUnmarshallingError{
	// 			Msg: fmt.Sprintf("can't find process instannce with key %d; "+
	// 				"the marshalled JSON was likely corrupt", t.ProcessInstanceKey),
	// 		}
	// 	}
	// 	t.baseElement = BPMN20.FindFlowNodesById(&pi.ProcessInfo.definitions, t.ElementId)[0]
	// 	availableOriginActivity := pi.findActivity(t.originActivity.Key())
	// 	if availableOriginActivity != nil {
	// 		t.originActivity = availableOriginActivity
	// 	} else {
	// 		originActivitySurrogate := t.originActivity.(activitySurrogate)
	// 		originActivitySurrogate.elementReference = BPMN20.FindFlowNodesById(&pi.ProcessInfo.definitions, originActivitySurrogate.ElementReferenceId)[0]
	// 		t.originActivity = originActivitySurrogate
	// 	}
	// }
	return nil
}

func recoverMessageSubscriptions(state *Engine) error {
	// for _, ms := range state.messageSubscriptions {
	// 	pi := state.FindProcessInstance(ms.ProcessInstanceKey)
	// 	if pi == nil {
	// 		return &BpmnEngineUnmarshallingError{
	// 			Msg: fmt.Sprintf("can't find process instannce with key %d; "+
	// 				"the marshalled JSON was likely corrupt", ms.ProcessInstanceKey),
	// 		}
	// 	}
	// 	ms.baseElement = BPMN20.FindFlowNodesById(&pi.ProcessInfo.definitions, ms.ElementId)[0]
	// 	availableOriginActivity := pi.findActivity(ms.originActivity.Key())
	// 	if availableOriginActivity != nil {
	// 		ms.originActivity = availableOriginActivity
	// 	} else {
	// 		originActivitySurrogate := ms.originActivity.(activitySurrogate)
	// 		originActivitySurrogate.elementReference = BPMN20.FindFlowNodesById(&pi.ProcessInfo.definitions, originActivitySurrogate.ElementReferenceId)[0]
	// 		ms.originActivity = originActivitySurrogate
	// 	}
	// }
	return nil
}

func createReferences(processes []*runtime.ProcessDefinition) (result []processInfoReference) {
	for _, pi := range processes {
		ref := processInfoReference{
			BpmnProcessId:    pi.BpmnProcessId,
			ProcessKey:       pi.ProcessKey,
			BpmnData:         pi.BpmnData,
			BpmnResourceName: pi.BpmnResourceName,
			BpmnChecksum:     hex.EncodeToString(pi.BpmnChecksum[:]),
		}
		result = append(result, ref)
	}
	return result
}

func (state *Engine) findProcess(processKey int64) *runtime.ProcessDefinition {
	// for i := 0; i < len(state.processes); i++ {
	// 	process := state.processes[i]
	// 	if process.ProcessKey == processKey {
	// 		return process
	// 	}
	// }
	return nil
}
