package bpmn20

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unicode"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
)

// listenerPositionRule tells the modeller where a listener is run.
const listenerPositionRule = `only a start event directly under the process takes an execution listener, with eventType="end"`

// listenerPathRule tells the modeller where in its element a listener sits.
const listenerPathRule = `a listener has to sit in an executionListeners element directly inside the extensionElements of its element`

// listenerOwnerPathRule tells the modeller where the element of a listener sits.
const listenerOwnerPathRule = `an element with a listener has to sit directly in a process or in a sub process of its flow`

// sitsInTheProcessFlow reports whether an element inside the given open
// elements is read by the model: one directly in a process of the
// definitions, or in sub-processes nested directly in one
// (TFlowElementsContainer). A flow node anywhere else, such as inside
// extension elements, is dropped by the model, together with any sub-process
// holding it there.
func sitsInTheProcessFlow(ancestors []openElement) bool {
	if len(ancestors) < 2 || ancestors[0].name != "definitions" || ancestors[1].name != "process" {
		return false
	}
	for _, ancestor := range ancestors[2:] {
		if ancestor.name != "subProcess" {
			return false
		}
	}
	return true
}

// ValidateListenerPositions refuses an execution listener on an element whose
// model does not read listeners, one which does not sit where the model reads
// it (extensionElements > executionListeners > executionListener), one on an
// element outside the flow of a process (see sitsInTheProcessFlow), and every
// task listener. Such a listener would be dropped without a word, so the raw
// XML is scanned; a listener the model reads is checked by
// ValidateForDeployment instead. A listener on the process element is left
// alone, wherever it sits in its extensionElements: the engine neither runs
// nor refuses it, as before execution listeners were supported. Every
// namespace prefix is read alike, as for every other extension element. The
// listeners are visited in document order, so the same model always names the
// same element. Malformed XML is left to the parser.
func ValidateListenerPositions(xmlData []byte) error {
	// every listener element's local name contains the word, whatever its
	// prefix, so a model without one is not scanned
	if !bytes.Contains(xmlData, []byte("Listener")) {
		return nil
	}
	decoder := xml.NewDecoder(bytes.NewReader(xmlData))
	var open []openElement
	for {
		token, err := decoder.Token()
		if err != nil {
			// io.EOF ends a well-formed document; any other error is the parser's to report
			return nil
		}
		switch element := token.(type) {
		case xml.StartElement:
			switch element.Name.Local {
			case "taskListener":
				owner := ownerOfListener(open)
				return fmt.Errorf("task listener on %s id=%q is not supported: the engine runs no task listeners", xmlNameInWords(owner.name), owner.id)
			case "executionListener":
				if err := validateListenerPosition(open, element); err != nil {
					return err
				}
			}
			open = append(open, openElement{name: element.Name.Local, id: attributeOf(element, "id")})
		case xml.EndElement:
			if len(open) > 0 {
				open = open[:len(open)-1]
			}
		}
	}
}

// validateListenerPosition refuses an execution listener, inside the given
// open elements, which the model would drop: one on an element whose model
// does not read listeners, one which does not sit where the model reads it,
// and one on an element which itself sits where the model does not read it.
func validateListenerPosition(open []openElement, listener xml.StartElement) error {
	owner := ownerOfListener(open)
	if owner.name == "process" {
		return nil
	}
	where := fmt.Sprintf("%s on %s id=%q", listenerInWords(listener), xmlNameInWords(owner.name), owner.id)
	if !modelReadsListenersOf(owner.name) {
		return fmt.Errorf("%s is not supported: %s", where, listenerPositionRule)
	}
	depth := len(open)
	if depth < 3 || open[depth-1].name != "executionListeners" || open[depth-2].name != "extensionElements" || open[depth-3] != owner {
		return fmt.Errorf("%s is not supported where it sits: %s", where, listenerPathRule)
	}
	if !sitsInTheProcessFlow(open[:depth-3]) {
		return fmt.Errorf("%s is not supported where the %s sits: %s", where, xmlNameInWords(owner.name), listenerOwnerPathRule)
	}
	return nil
}

// listenerInWords names an execution listener of the raw XML by its event
// type, or says it has none, as a listener in the shape of Camunda 7, which
// names its event in an "event" attribute.
func listenerInWords(listener xml.StartElement) string {
	if eventType := attributeOf(listener, "eventType"); eventType != "" {
		return fmt.Sprintf("execution listener eventType=%q", eventType)
	}
	return "execution listener without an eventType"
}

// modelReadsListenersOf reports whether the model of the XML element of the
// given local name reads its execution listeners, that is whether the element
// is a flow node of the model.
func modelReadsListenersOf(elementName string) bool {
	return flowNodeTags[elementName]
}

// attributeOf answers the value of the element's attribute without a
// namespace, or "".
func attributeOf(element xml.StartElement, name string) string {
	for _, attribute := range element.Attr {
		if attribute.Name.Local == name && attribute.Name.Space == "" {
			return attribute.Value
		}
	}
	return ""
}

// openElement is an element of the raw XML whose end tag was not read yet.
type openElement struct {
	name string
	id   string
}

// ownerOfListener walks up from a listener to the BPMN element owning it: the
// first element whose model reads listeners, or else the element of the first
// extensionElements on the way, however deep in other extension elements the
// listener sits. A listener outside both belongs to the element holding it,
// past the listener containers.
func ownerOfListener(open []openElement) openElement {
	for i := len(open) - 1; i >= 0; i-- {
		if modelReadsListenersOf(open[i].name) {
			return open[i]
		}
		if open[i].name == "extensionElements" && i > 0 {
			return open[i-1]
		}
	}
	i := len(open) - 1
	for i >= 0 && slices.Contains([]string{"executionListeners", "taskListeners"}, open[i].name) {
		i--
	}
	if i < 0 {
		return openElement{}
	}
	return open[i]
}

// xmlNameInWords answers the local name of an XML element as a message names
// it, such as "start event" for startEvent.
func xmlNameInWords(name string) string {
	var words strings.Builder
	for i, r := range name {
		if unicode.IsUpper(r) {
			if i > 0 {
				words.WriteByte(' ')
			}
			r = unicode.ToLower(r)
		}
		words.WriteRune(r)
	}
	return words.String()
}

// validateExecutionListeners checks the listeners of every flow node of the
// process (see TProcess.CheckListeners), in the order forEachFlowNode visits
// them, so the same model always names the same element.
func validateExecutionListeners(process *TProcess) error {
	var refusal error
	forEachFlowNode(&process.TFlowElementsContainer, func(node FlowNode) {
		if refusal == nil {
			refusal = process.CheckListeners(node)
		}
	})
	return refusal
}

// CheckListeners refuses the listeners of a flow node which the engine cannot
// run as the model says: on an element without an id, or whose id another
// flow node shares, since the engine finds the element of a listener by its
// id; a listener of an event type the engine does not run there (see
// ListenerEventTypesRunOn); more than one listener; on an element without an
// outgoing sequence flow, which the token could not leave; and a type,
// retries or retryBackoff which is not a literal the engine reads. Literals
// only, so that creating a listener's job never fails on an expression. A
// deployment checks every flow node with it; the engine checks an element of
// a definition stored without that check before it runs its listeners.
func (p *TProcess) CheckListeners(node FlowNode) error {
	listeners := node.GetExecutionListeners()
	if len(listeners) == 0 {
		return nil
	}
	element := flowNodeInWords(node)
	if node.GetId() == "" {
		return fmt.Errorf("%s without an id carries an execution listener: give every element a unique id", element)
	}
	where := fmt.Sprintf("%s id=%q", element, node.GetId())
	if p.flowNodeIDCounts[node.GetId()] > 1 {
		return fmt.Errorf("%s carries an execution listener, but another flow node of the process has the same id: give every element a unique id", where)
	}
	runnableEventTypes := p.ListenerEventTypesRunOn(node)
	rule := listenerPositionRule
	if len(runnableEventTypes) > 0 {
		rule = fmt.Sprintf("a %s takes one listener with eventType=%q", element, strings.Join(runnableEventTypes, `" or "`))
	}
	for _, listener := range listeners {
		if !slices.Contains(runnableEventTypes, listener.EventType) {
			return fmt.Errorf("execution listener eventType=%q on %s is not supported: %s", listener.EventType, where, rule)
		}
	}
	if len(listeners) > 1 {
		return fmt.Errorf("%s has %d execution listeners, which is not supported: %s", where, len(listeners), rule)
	}
	if len(node.GetOutgoingAssociation()) == 0 {
		return fmt.Errorf("%s has an execution listener but no outgoing sequence flow: the token could not leave it when the listener's job is completed", where)
	}
	for _, listener := range listeners {
		if err := ValidateListenerLiterals(listener); err != nil {
			return fmt.Errorf("invalid execution listener eventType=%q on %s: %w", listener.EventType, where, err)
		}
	}
	return nil
}

// ListenerIDIsAmbiguous reports whether several flow nodes of the process
// have the id, and one of them carries a listener the engine runs. A token at
// such an id cannot tell which element it is at, so the listener could run
// for the other element, or not at all. A deployment refuses such a model.
func (p *TProcess) ListenerIDIsAmbiguous(id string) bool {
	if p.flowNodeIDCounts[id] < 2 {
		return false
	}
	ambiguous := false
	forEachFlowNode(&p.TFlowElementsContainer, func(node FlowNode) {
		ambiguous = ambiguous || node.GetId() == id && p.RunsListenersOn(node)
	})
	return ambiguous
}

// RunsExecutionListeners reports whether the process carries a listener the
// engine runs (see TProcess.ListenerEventTypesRunOn).
func (definitions *TDefinitions) RunsExecutionListeners() bool {
	process := &definitions.Process
	runs := false
	forEachFlowNode(&process.TFlowElementsContainer, func(node FlowNode) {
		runs = runs || process.RunsListenersOn(node)
	})
	return runs
}

// flowNodeType is the interface every flow node of the model implements.
var flowNodeType = reflect.TypeFor[FlowNode]()

// flowNodeTags holds the local names of the XML elements the model reads as
// flow nodes of a process or sub-process, boundary events included.
var flowNodeTags = func() map[string]bool {
	tags := make(map[string]bool)
	container := reflect.TypeFor[TFlowElementsContainer]()
	for i := range container.NumField() {
		if field := container.Field(i); holdsFlowNodes(field.Type) {
			tags[field.Tag.Get("xml")] = true
		}
	}
	return tags
}()

// holdsFlowNodes reports whether a field of a container is a list of flow nodes.
func holdsFlowNodes(fieldType reflect.Type) bool {
	return fieldType.Kind() == reflect.Slice && reflect.PointerTo(fieldType.Elem()).Implements(flowNodeType)
}

// forEachFlowNode visits every flow node of the container and of its
// sub-processes, boundary events included, which the id index of the process
// leaves out. The nodes of a container are visited in the order of the
// container's fields and, within one field, of the document; the content of a
// sub-process follows the container holding it.
func forEachFlowNode(container *TFlowElementsContainer, visit func(node FlowNode)) {
	fields := reflect.ValueOf(container).Elem()
	for i := range fields.NumField() {
		field := fields.Field(i)
		if !holdsFlowNodes(field.Type()) {
			continue
		}
		for j := range field.Len() {
			visit(field.Index(j).Addr().Interface().(FlowNode))
		}
	}
	for i := range container.SubProcess {
		forEachFlowNode(&container.SubProcess[i].TFlowElementsContainer, visit)
	}
}

// flowNodeInWords names the kind of a flow node as a message names it, such
// as "start event" for a TStartEvent, from the name of its XML element:
// several kinds of task answer the same element type.
func flowNodeInWords(node FlowNode) string {
	return xmlNameInWords(bpmnElementName(reflect.Indirect(reflect.ValueOf(node)).Type().Name()))
}

// ValidateListenerLiterals refuses a listener whose type is blank or an
// expression, or whose retries or retryBackoff is an expression or a literal
// which does not parse. A blank retries or retryBackoff takes the engine's
// default.
func ValidateListenerLiterals(listener extensions.TExecutionListener) error {
	if strings.TrimSpace(listener.Type) == "" {
		return errors.New("its type is blank: a listener needs the job type as a literal")
	}
	if extensions.IsExpression(listener.Type) {
		return fmt.Errorf("its type %q is an expression: a listener's type must be a literal", listener.Type)
	}
	if extensions.IsExpression(strings.TrimSpace(listener.Retries)) {
		return fmt.Errorf("its retries %q is an expression: a listener's retries must be a literal", listener.Retries)
	}
	if extensions.IsExpression(strings.TrimSpace(listener.RetryBackoff)) {
		return fmt.Errorf("its retryBackoff %q is an expression: a listener's retryBackoff must be a literal", listener.RetryBackoff)
	}
	return validateRetryLiterals(listener.Retries, listener.RetryBackoff)
}
