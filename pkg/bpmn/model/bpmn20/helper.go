package bpmn20

import (
	"encoding/xml"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func (definitions *TDefinitions) ResolveReferences() error {
	byType := make(map[string][]string)
	collectUnknownElements(&definitions.Process.TFlowElementsContainer, byType)
	for name, ids := range byType {
		return fmt.Errorf("unsupported element type '%s' (ids: %v): use supported elements only", name, ids)
	}
	if err := walkForUnsupportedEventDefinitions(&definitions.Process); err != nil {
		return err
	}
	if err := validateSubProcessStartEvents(&definitions.Process.TFlowElementsContainer); err != nil {
		return err
	}
	// Build per-TFlowElementsContainer subtree indices for O(1) lookup of
	// flow nodes and internal tasks. Each container level's index contains
	// its own elements plus all elements owned by descendant sub-processes,
	// so a lookup against a sub-process is automatically scoped to that
	// sub-process's visible subtree (it can never resolve ancestors or
	// sibling branches). Lookup is O(1) regardless of total process size
	// or number of sub-processes.
	populateContainerIndex(&definitions.Process.TFlowElementsContainer)
	baseElementMap := make(map[string]BaseElement)
	resolvables := make([]resolvableFunc, 0)
	err := collectBaseElements(definitions, &baseElementMap, &resolvables)
	if err != nil {
		return fmt.Errorf("failed to collect references: %w", err)
	}
	definitions.baseElements = baseElementMap
	for _, resolvable := range resolvables {
		if err = resolvable(&baseElementMap); err != nil {
			return fmt.Errorf("failed to resolve references: %w", err)
		}
	}
	if err := validateEventBasedGateways(&definitions.Process.TFlowElementsContainer); err != nil {
		return err
	}
	return nil
}

var eventDefinitionIfaceType = reflect.TypeOf((*EventDefinition)(nil)).Elem()

func walkForUnsupportedEventDefinitions(element any) error {
	val := reflect.ValueOf(element)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if !val.IsValid() || val.Kind() != reflect.Struct {
		return nil
	}
	for i := range val.NumField() {
		fieldVal := val.Field(i)
		fieldType := val.Type().Field(i)

		if fieldVal.Kind() == reflect.Interface && fieldType.Type.Implements(eventDefinitionIfaceType) {
			if !fieldVal.IsNil() {
				if u, ok := fieldVal.Interface().(TUnsupportedEventDefinition); ok {
					elementName := bpmnElementName(val.Type().Name())
					var id string
					if idField := val.FieldByName("Id"); idField.IsValid() {
						id = idField.String()
					}
					return fmt.Errorf("unsupported element configuration: %s id=%q has unsupported event definition '%s'",
						elementName, id, u.Name)
				}
			}
			continue
		}
		if fieldVal.Kind() == reflect.Slice {
			for j := range fieldVal.Len() {
				el := fieldVal.Index(j)
				if el.Kind() == reflect.Struct {
					if err := walkForUnsupportedEventDefinitions(el.Addr().Interface()); err != nil {
						return err
					}
				}
			}
		} else if fieldVal.Kind() == reflect.Struct {
			if err := walkForUnsupportedEventDefinitions(fieldVal.Addr().Interface()); err != nil {
				return err
			}
		}
	}
	return nil
}

func bpmnElementName(typeName string) string {
	if len(typeName) > 1 && typeName[0] == 'T' {
		typeName = typeName[1:]
	}
	if len(typeName) == 0 {
		return typeName
	}
	return strings.ToLower(typeName[:1]) + typeName[1:]
}

func validateEventBasedGateways(container *TFlowElementsContainer) error {
	for _, gw := range container.EventBasedGateway {
		for _, flow := range gw.GetOutgoingAssociation() {
			if flow == nil {
				continue
			}
			if flow.GetConditionExpression() != "" {
				return fmt.Errorf("unsupported element configuration: eventBasedGateway id=%q has conditional outgoing flow '%s'", gw.GetId(), flow.GetId())
			}
			if _, ok := flow.GetTargetRef().(*TIntermediateCatchEvent); !ok {
				return fmt.Errorf("unsupported element configuration: eventBasedGateway id=%q has non-IntermediateCatchEvent target '%s'", gw.GetId(), flow.GetTargetRef().GetId())
			}
		}
	}
	for i := range container.SubProcess {
		if err := validateEventBasedGateways(&container.SubProcess[i].TFlowElementsContainer); err != nil {
			return err
		}
	}
	return nil
}

func validateSubProcessStartEvents(container *TFlowElementsContainer) error {
	for i := range container.SubProcess {
		subProcess := &container.SubProcess[i]
		if len(subProcess.StartEvents) > 1 {
			ids := make([]string, 0, len(subProcess.StartEvents))
			for _, startEvent := range subProcess.StartEvents {
				ids = append(ids, startEvent.GetId())
			}
			return fmt.Errorf("invalid sub process configuration: subProcess id=%q declares %d start events (ids: %v): a sub process must declare exactly one start event",
				subProcess.GetId(), len(subProcess.StartEvents), ids)
		}
		for i := range subProcess.StartEvents {
			startEvent := &subProcess.StartEvents[i]
			if !startEvent.IsInterrupting && hasErrorEventDefinition(startEvent) {
				return fmt.Errorf("invalid sub process configuration: subProcess id=%q has a non-interrupting error start event id=%q: error start events must be interrupting",
					subProcess.GetId(), startEvent.GetId())
			}
		}
		if err := validateSubProcessStartEvents(&subProcess.TFlowElementsContainer); err != nil {
			return err
		}
	}
	return nil
}

func hasErrorEventDefinition(startEvent *TStartEvent) bool {
	for _, eventDefinition := range startEvent.EventDefinitions {
		if _, ok := eventDefinition.(TErrorEventDefinition); ok {
			return true
		}
	}
	return false
}

func collectUnknownElements(container *TFlowElementsContainer, byType map[string][]string) {
	for _, e := range container.UnknownElements {
		if len(e.Incoming) > 0 || len(e.Outgoing) > 0 {
			byType[e.XMLName.Local] = append(byType[e.XMLName.Local], e.Id)
		}
	}
	for i := range container.SubProcess {
		collectUnknownElements(&container.SubProcess[i].TFlowElementsContainer, byType)
	}
}

func (definitions *TDefinitions) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	// Create an alias to avoid recursion
	type Alias TDefinitions
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(definitions),
	}

	// Unmarshal into the alias
	if err := d.DecodeElement(aux, &start); err != nil {
		return fmt.Errorf("failed to unmarshal TDefinitions: %w", err)
	}

	// Resolve references after unmarshalling
	if err := definitions.ResolveReferences(); err != nil {
		return fmt.Errorf("failed to resolve references: %w", err)
	}
	return nil
}

type resolvableFunc func(refs *map[string]BaseElement) error

// populateContainerIndex fills container.flowNodesByID with this
// container's own elements plus every descendant element from nested
// sub-processes, so a single map probe resolves any element in the
// container's subtree. Boundary events are excluded to preserve
// historical lookup semantics.
func populateContainerIndex(container *TFlowElementsContainer) {
	container.flowNodesByID = make(map[string]FlowNode)
	container.internalTasksByID = make(map[string]InternalTask)

	register := func(id string, node FlowNode) {
		if id == "" {
			return
		}
		container.flowNodesByID[id] = node
		if task, ok := node.(InternalTask); ok {
			container.internalTasksByID[id] = task
		}
	}

	for i := range container.StartEvents {
		register(container.StartEvents[i].GetId(), &container.StartEvents[i])
	}
	for i := range container.EndEvents {
		register(container.EndEvents[i].GetId(), &container.EndEvents[i])
	}
	for i := range container.ServiceTasks {
		register(container.ServiceTasks[i].GetId(), &container.ServiceTasks[i])
	}
	for i := range container.UserTasks {
		register(container.UserTasks[i].GetId(), &container.UserTasks[i])
	}
	for i := range container.BusinessRuleTask {
		register(container.BusinessRuleTask[i].GetId(), &container.BusinessRuleTask[i])
	}
	for i := range container.SendTask {
		register(container.SendTask[i].GetId(), &container.SendTask[i])
	}
	for i := range container.ReceiveTask {
		register(container.ReceiveTask[i].GetId(), &container.ReceiveTask[i])
	}
	for i := range container.ParallelGateway {
		register(container.ParallelGateway[i].GetId(), &container.ParallelGateway[i])
	}
	for i := range container.ExclusiveGateway {
		register(container.ExclusiveGateway[i].GetId(), &container.ExclusiveGateway[i])
	}
	for i := range container.EventBasedGateway {
		register(container.EventBasedGateway[i].GetId(), &container.EventBasedGateway[i])
	}
	for i := range container.InclusiveGateway {
		register(container.InclusiveGateway[i].GetId(), &container.InclusiveGateway[i])
	}
	for i := range container.IntermediateCatchEvent {
		register(container.IntermediateCatchEvent[i].GetId(), &container.IntermediateCatchEvent[i])
	}
	for i := range container.IntermediateThrowEvent {
		register(container.IntermediateThrowEvent[i].GetId(), &container.IntermediateThrowEvent[i])
	}
	for i := range container.CallActivity {
		register(container.CallActivity[i].GetId(), &container.CallActivity[i])
	}
	for i := range container.SubProcess {
		sp := &container.SubProcess[i]
		register(sp.GetId(), sp)
	}
	for i := range container.SubProcess {
		sp := &container.SubProcess[i]
		populateContainerIndex(&sp.TFlowElementsContainer)
		for k, v := range sp.flowNodesByID {
			if _, exists := container.flowNodesByID[k]; !exists {
				container.flowNodesByID[k] = v
			}
		}
		for k, v := range sp.internalTasksByID {
			if _, exists := container.internalTasksByID[k]; !exists {
				container.internalTasksByID[k] = v
			}
		}
	}
}

func collectBaseElements(element interface{}, refs *map[string]BaseElement, resolvables *[]resolvableFunc) error {
	val := reflect.ValueOf(element)

	// If c is a pointer receiver, adjust:
	baseElement, ok := val.Interface().(BaseElement)
	if ok {
		// already registered
		if _, ok := (*refs)[baseElement.GetId()]; !ok {
			(*refs)[baseElement.GetId()] = baseElement
		}
	}

	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if !val.IsValid() || val.Kind() != reflect.Struct {
		return nil // Skip invalid or non-struct values
	}
	baseElementType := reflect.TypeOf((*BaseElement)(nil)).Elem()
	for i := range val.NumField() {
		fieldVal := val.Field(i)
		// check if the field requires reference resolution
		if idFieldName := val.Type().Field(i).Tag.Get("idField"); idFieldName != "" {
			if idField := val.FieldByName(idFieldName); idField.IsValid() {
				// assert that id field of string or []string type
				if idField.Kind() != reflect.String && (idField.Kind() != reflect.Slice || idField.Type().Elem().Kind() != reflect.String) {
					return fmt.Errorf("ID containing field [%s] in structure [%s] has to be 'string' or '[]string' type", idFieldName, val.Type().Name())
				}
				// assert that reference field of <Interface> or []<Interface> type where BaseElement is assignable to <Interface>
				if (fieldVal.Kind() != reflect.Interface || !fieldVal.Type().Implements(baseElementType)) &&
					(fieldVal.Kind() != reflect.Slice || !fieldVal.Type().Elem().Implements(baseElementType)) {
					return fmt.Errorf("field [%s] in structure [%s] has to be interface or slice of interfaces assignable from BaseElement'", val.Type().Field(i).Name, val.Type().Name())
				}
				*resolvables = append(*resolvables, makeResolvable(fieldVal, val.FieldByName(idFieldName)))
			} else {
				return fmt.Errorf("field %s containing IDs not found in struct", idFieldName)
			}
		}

		if fieldVal.Kind() == reflect.Slice {
			for j := range fieldVal.Len() {
				arrEl := fieldVal.Index(j)
				if !arrEl.CanInterface() || arrEl.Kind() != reflect.Struct {
					continue
				}
				var err = collectBaseElements(arrEl.Addr().Interface(), refs, resolvables)
				if err != nil {
					return err
				}
			}
		} else {
			if !fieldVal.CanInterface() || fieldVal.Kind() != reflect.Struct {
				continue
			}
			var err = collectBaseElements(fieldVal.Addr().Interface(), refs, resolvables)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func makeResolvable(fieldVal reflect.Value, idField reflect.Value) func(refs *map[string]BaseElement) error {
	singleIDprocessor := func(fieldVal reflect.Value, idField reflect.Value, refs *map[string]BaseElement, setter func(value reflect.Value) error) error {
		id := idField.String()
		if id == "" {
			// skip is ID is empty
			return nil
		}
		baseEl, ok := (*refs)[id]
		if !ok {
			return fmt.Errorf("no registered BaseElement with ID [%s]", id)
		}
		val := reflect.ValueOf(baseEl)
		return setter(val)
	}
	return func(refs *map[string]BaseElement) error {
		switch fieldVal.Kind() {
		case reflect.Slice:
			var joinErr error
			for i := range idField.Len() {
				id := idField.Index(i)
				err := singleIDprocessor(fieldVal, id, refs, func(value reflect.Value) error {
					if fieldVal.IsNil() {
						fieldVal.Set(reflect.MakeSlice(fieldVal.Type(), 0, idField.Len()))
					}
					if value.Type().AssignableTo(fieldVal.Type().Elem()) {
						fieldVal.Set(reflect.Append(fieldVal, value))
					} else {
						return fmt.Errorf("resolved reference with ID [%s] is not assignable to %s", id, fieldVal.Elem().Type().Name())
					}
					return nil
				})
				if err != nil {
					joinErr = errors.Join(joinErr, fmt.Errorf("error processing %s[%d] type %s: %w", fieldVal.Type(), i, id.String(), err))
				}
			}
			if joinErr != nil {
				return joinErr
			}
		case reflect.Interface:
			id := idField
			return singleIDprocessor(fieldVal, id, refs, func(value reflect.Value) error {
				if value.Type().AssignableTo(fieldVal.Type()) {
					fieldVal.Set(value)
				} else {
					return fmt.Errorf("resolved reference with ID [%s] is not assignable to %s", id, fieldVal.Type().Name())
				}
				return nil
			})
		default:
			return fmt.Errorf("error in structure [%s]: field is not of a slice or interface type", fieldVal.Type().Name())
		}
		return nil
	}
}

func FindBoundaryEventsForActivity(processContainer *TFlowElementsContainer, activityId string) (result []TBoundaryEvent) {
	for _, boundaryEvent := range processContainer.BoundaryEvent {
		if boundaryEvent.AttachedToRef == activityId {
			result = append(result, boundaryEvent)
		}
	}
	if len(result) != 0 {
		return result
	}
	for _, subProcess := range processContainer.SubProcess {
		if res := FindBoundaryEventsForActivity(&subProcess.TFlowElementsContainer, activityId); res != nil {
			result = append(result, res...)
			return result
		}
	}
	return result
}

func FindBaseElementById(definitions *TDefinitions, id string) (BaseElement, bool) {
	v, ok := definitions.baseElements[id]
	return v, ok
}

// FindEventSubProcesses returns (non-recursively) all subprocesses with TriggeredByEvent=true in the given flow elements container.
func FindEventSubProcesses(container *TFlowElementsContainer) []*TSubProcess {
	result := make([]*TSubProcess, 0)
	for i := range container.SubProcess {
		if container.SubProcess[i].TriggeredByEvent {
			result = append(result, &container.SubProcess[i])
		}
	}
	return result
}
