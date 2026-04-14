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
	// Map to store FlowNodes by their IDs
	baseElementMap := make(map[string]BaseElement)
	resolvables := make([]resolvableFunc, 0)
	err := collectBaseElements(definitions, &baseElementMap, &resolvables)
	if err != nil {
		return fmt.Errorf("failed to collect references: %w", err)
	}
	definitions.baseElements = baseElementMap
	// Try to resolve references for each base element implementing ResolvableReferences
	for _, resolvable := range resolvables {
		// Check if the baseElement implements ResolvableReferences
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
