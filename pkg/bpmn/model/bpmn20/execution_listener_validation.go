package bpmn20

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"maps"
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
const listenerOwnerPathRule = `an element with a listener has to sit directly in a process or a sub process`

// flowElementContainers are the XML elements whose model reads the flow nodes
// directly inside them (TFlowElementsContainer); a flow node anywhere else,
// such as inside extension elements, is dropped by the model.
var flowElementContainers = []string{"process", "subProcess"}

// ValidateListenerPositions refuses an execution listener on an element whose
// model does not read listeners, one which does not sit where the model reads
// it (extensionElements > executionListeners > executionListener), one on an
// element outside a process or sub-process, and every task listener. Such a listener would be dropped without a word, so the raw
// XML is scanned; a listener the model reads is checked by
// ValidateForDeployment instead. A listener on the process element is left
// alone, wherever it sits in it: the engine neither runs nor refuses it, as
// before execution listeners were supported. Every namespace prefix is read
// alike, as for every other extension element. The listeners are visited in
// document order, so the same model always names the same element. Malformed
// XML is left to the parser.
func ValidateListenerPositions(xmlData []byte) error {
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
	if depth < 4 || !slices.Contains(flowElementContainers, open[depth-4].name) {
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

// ownerOfListener walks up from a listener through the elements which hold
// listeners (extensionElements and the listener containers) to the BPMN
// element owning it.
func ownerOfListener(open []openElement) openElement {
	i := len(open) - 1
	for i >= 0 && slices.Contains([]string{"extensionElements", "executionListeners", "taskListeners"}, open[i].name) {
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
// model reads them, in the order of the element ids, so the same model always
// names the same element. It refuses a listener the engine does not run there
// (see TProcess.RunnableListenerEventTypes), more than one listener, and a
// type, retries or retryBackoff which is not a literal the engine reads.
// Literals only, so that creating a listener's job never fails on an
// expression.
func validateExecutionListeners(process *TProcess) error {
	for _, id := range slices.Sorted(maps.Keys(process.flowNodesByID)) {
		owner, ok := process.flowNodesByID[id].(ExecutionListenerOwner)
		if !ok || len(owner.GetExecutionListeners()) == 0 {
			continue
		}
		if err := validateListenersOf(owner, process.RunnableListenerEventTypes(owner)); err != nil {
			return err
		}
	}
	return nil
}

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
	if retries := strings.TrimSpace(listener.Retries); retries != "" {
		if extensions.IsExpression(retries) {
			return fmt.Errorf("its retries %q is an expression: a listener's retries must be a literal", listener.Retries)
		}
		if _, err := extensions.ParseRetries(retries); err != nil {
			return err
		}
	}
	if backoff := strings.TrimSpace(listener.RetryBackoff); backoff != "" {
		if extensions.IsExpression(backoff) {
			return fmt.Errorf("its retryBackoff %q is an expression: a listener's retryBackoff must be a literal", listener.RetryBackoff)
		}
		if _, err := extensions.ParseRetryBackoff(backoff); err != nil {
			return err
		}
	}
	return nil
}

// HasExecutionListeners reports whether an element of the process, at any
// depth, carries an execution listener the model reads.
func (definitions *TDefinitions) HasExecutionListeners() bool {
	for _, node := range definitions.Process.flowNodesByID {
		if owner, ok := node.(ExecutionListenerOwner); ok && len(owner.GetExecutionListeners()) > 0 {
			return true
		}
	}
	return false
}
