package bpmn20

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
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
// is an ExecutionListenerOwner. An element which becomes one is added here.
func modelReadsListenersOf(elementName string) bool {
	return elementName == "startEvent"
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
// it, such as "start event" for startEvent, to read as elementTypeInWords.
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

// validateExecutionListeners checks the listeners of every element whose
// model reads them, in document order within each container, so the same
// model always names the same element. It refuses a listener on an element
// whose id another element of the process shares, since the engine finds the
// element of a listener by its id; a listener the engine does not run there
// (see TProcess.RunnableListenerEventTypes); more than one listener; and a
// type, retries or retryBackoff which is not a literal the engine reads.
// Literals only, so that creating a listener's job never fails on an
// expression.
func validateExecutionListeners(process *TProcess) error {
	for _, owner := range listenerOwnersOf(&process.TFlowElementsContainer) {
		if len(owner.GetExecutionListeners()) == 0 {
			continue
		}
		if process.GetFlowNodeById(owner.GetId()) != FlowNode(owner) {
			return fmt.Errorf("%s id=%q carries an execution listener, but another element of the process has the same id: give every element a unique id",
				elementTypeInWords(owner.GetType()), owner.GetId())
		}
		if err := validateListenersOf(owner, process.RunnableListenerEventTypes(owner)); err != nil {
			return err
		}
	}
	return nil
}

// listenerOwnersOf answers every element of the container, its sub-processes
// included, whose model reads execution listeners (see
// modelReadsListenersOf). The containers are walked, not the id index of the
// process, which holds only one element of an id that several elements share.
func listenerOwnersOf(container *TFlowElementsContainer) []ExecutionListenerOwner {
	owners := make([]ExecutionListenerOwner, 0, len(container.StartEvents))
	for i := range container.StartEvents {
		owners = append(owners, &container.StartEvents[i])
	}
	for i := range container.SubProcess {
		owners = append(owners, listenerOwnersOf(&container.SubProcess[i].TFlowElementsContainer)...)
	}
	return owners
}

// validateListenersOf refuses the listeners of one owner which the engine
// does not run there: a listener of an event type outside runnableEventTypes
// (none outside the positions of TProcess.RunnableListenerEventTypes), more
// than one listener, and a listener whose attributes are not literals (see
// ValidateListenerLiterals).
func validateListenersOf(owner ExecutionListenerOwner, runnableEventTypes []string) error {
	where := fmt.Sprintf("%s id=%q", elementTypeInWords(owner.GetType()), owner.GetId())
	rule := listenerPositionRule
	if len(runnableEventTypes) > 0 {
		rule = fmt.Sprintf("a %s takes one listener with eventType=%q", elementTypeInWords(owner.GetType()), strings.Join(runnableEventTypes, `" or "`))
	}
	listeners := owner.GetExecutionListeners()
	for _, listener := range listeners {
		if !slices.Contains(runnableEventTypes, listener.EventType) {
			return fmt.Errorf("execution listener eventType=%q on %s is not supported: %s", listener.EventType, where, rule)
		}
	}
	if len(listeners) > 1 {
		return fmt.Errorf("%s has %d execution listeners, which is not supported: %s", where, len(listeners), rule)
	}
	if len(owner.GetOutgoingAssociation()) == 0 {
		return fmt.Errorf("%s has an execution listener but no outgoing sequence flow: the token could not leave it when the listener's job is completed", where)
	}
	for _, listener := range listeners {
		if err := ValidateListenerLiterals(listener); err != nil {
			return fmt.Errorf("invalid execution listener eventType=%q on %s: %w", listener.EventType, where, err)
		}
	}
	return nil
}

// elementTypeInWords answers an element type as a message names it, such as
// "start event" for START_EVENT.
func elementTypeInWords(elementType ElementType) string {
	return strings.ReplaceAll(strings.ToLower(string(elementType)), "_", " ")
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

// RunsExecutionListeners reports whether the process carries a listener the
// engine runs (see TProcess.RunnableListenerEventTypes).
func (definitions *TDefinitions) RunsExecutionListeners() bool {
	process := &definitions.Process
	for _, owner := range listenerOwnersOf(&process.TFlowElementsContainer) {
		for _, eventType := range process.RunnableListenerEventTypes(owner) {
			if ListenerTaskOf(owner, eventType) != nil {
				return true
			}
		}
	}
	return false
}
