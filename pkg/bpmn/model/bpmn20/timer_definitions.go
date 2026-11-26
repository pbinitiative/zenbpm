package bpmn20

import (
	"encoding/xml"
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
)

// Text is the value of a timeDate, timeDuration or timeCycle element as its
// author wrote it: XMLText holds the element's content as it stands in the
// file, where a FEEL expression such as =a < b is written with entities or in
// a CDATA section. Content which does not read as XML is returned as it is.
func (t *TTimeInfo) Text() string {
	if t == nil {
		return ""
	}
	decoder := xml.NewDecoder(strings.NewReader(t.XMLText))
	var text strings.Builder
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return strings.TrimSpace(t.XMLText)
		}
		if characters, ok := token.(xml.CharData); ok {
			text.Write(characters)
		}
	}
	return strings.TrimSpace(text.String())
}

// IsExpression reports whether the timer value is a FEEL expression, which
// starts with '='. Such a value is evaluated when its timer is created; any
// other value is a literal.
func (t *TTimeInfo) IsExpression() bool {
	return t != nil && extensions.IsExpression(t.Text())
}

// TimerDefinitionRef is a timer event definition together with the element
// which carries it.
type TimerDefinitionRef struct {
	ElementID  string
	Definition TTimerEventDefinition
	// ProcessLevelStart tells the start event of the process itself from the
	// start event of an event subprocess: its timer is created when the
	// definition is deployed, where no process instance and no variables exist.
	ProcessLevelStart bool
}

// TimerDefinitions lists the timer event definitions of the process and of
// its subprocesses at every depth: start events, intermediate catch events and
// boundary events. The elements come in the order of their ids, so the same
// model is always walked the same way.
func (definitions *TDefinitions) TimerDefinitions() []TimerDefinitionRef {
	refs := collectTimerDefinitions(&definitions.Process.TFlowElementsContainer, true, nil)
	sort.SliceStable(refs, func(i, j int) bool { return refs[i].ElementID < refs[j].ElementID })
	return refs
}

func collectTimerDefinitions(container *TFlowElementsContainer, processLevel bool, refs []TimerDefinitionRef) []TimerDefinitionRef {
	for i := range container.StartEvents {
		for _, eventDefinition := range container.StartEvents[i].EventDefinitions {
			if timer, ok := eventDefinition.(TTimerEventDefinition); ok {
				refs = append(refs, TimerDefinitionRef{
					ElementID:         container.StartEvents[i].GetId(),
					Definition:        timer,
					ProcessLevelStart: processLevel,
				})
			}
		}
	}
	for i := range container.IntermediateCatchEvent {
		if timer, ok := container.IntermediateCatchEvent[i].EventDefinition.(TTimerEventDefinition); ok {
			refs = append(refs, TimerDefinitionRef{ElementID: container.IntermediateCatchEvent[i].GetId(), Definition: timer})
		}
	}
	for i := range container.BoundaryEvent {
		if timer, ok := container.BoundaryEvent[i].EventDefinition.(TTimerEventDefinition); ok {
			refs = append(refs, TimerDefinitionRef{ElementID: container.BoundaryEvent[i].GetId(), Definition: timer})
		}
	}
	for i := range container.SubProcess {
		refs = collectTimerDefinitions(&container.SubProcess[i].TFlowElementsContainer, false, refs)
	}
	return refs
}
