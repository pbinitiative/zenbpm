package extensions

import (
	"fmt"
)

//TODO: This needs to be revised licensewise

type TIoMapping struct {
	Source string `xml:"source,attr"`
	Target string `xml:"target,attr"`
}

type TIn struct {
	BusinessKey *string `xml:"businessKey,attr"`
}

type TCalledElement struct {
	ProcessId   string  `xml:"processId,attr"`
	Version     *int32  `xml:"version,attr,omitempty"`
	BindingType *string `xml:"bindingType,attr,omitempty"`
	VersionTag  *string `xml:"versionTag,attr,omitempty"`
}

// VersionSelection describes which process definition a TCalledElement targets.
// It is consumed by the engine after BPMN parsing succeeds.
type VersionSelection struct {
	// Version, when non-nil, selects a numeric process version directly. The
	// value is positive and is resolved through FindProcessDefinitionByIDAndVersion.
	Version *int32
	// VersionTag, when non-empty, first selects the process definition whose
	// stored versionTag equals this value. If no exact tag exists, a value in the
	// v<positive-number> form selects that exact numeric process version.
	VersionTag string
}

// HasSelection reports whether any form of explicit version was configured.
func (selection VersionSelection) HasSelection() bool {
	return selection.Version != nil || selection.VersionTag != ""
}

// ResolveVersion interprets the called element attributes and returns the
// version selection the engine should use. Call Activity resolution proceeds
// from this result: explicit Version beats VersionTag; VersionTag tries an exact
// stored tag before its optional v<positive-number> interpretation. An unresolved
// explicit selection fails rather than falling back to latest.
func (calledElement TCalledElement) ResolveVersion() (VersionSelection, error) {
	if calledElement.BindingType == nil {
		if calledElement.Version == nil {
			// Zeebe convention: missing binding means latest. A stale versionTag
			// attribute is ignored unless bindingType explicitly selects it.
			return VersionSelection{}, nil
		}
		if *calledElement.Version <= 0 {
			return VersionSelection{}, fmt.Errorf("invalid process version %d: version must be greater than zero", *calledElement.Version)
		}
		version := *calledElement.Version
		return VersionSelection{Version: &version}, nil
	}

	switch *calledElement.BindingType {
	case "latest":
		if calledElement.Version != nil {
			return VersionSelection{}, fmt.Errorf(`numeric version cannot be combined with bindingType "latest"`)
		}
		return VersionSelection{}, nil
	case "versionTag":
		if calledElement.Version != nil {
			return VersionSelection{}, fmt.Errorf("cannot configure both numeric version and versionTag")
		}
		if calledElement.VersionTag == nil || *calledElement.VersionTag == "" {
			return VersionSelection{}, fmt.Errorf(`invalid versionTag: bindingType "versionTag" requires versionTag`)
		}
		return VersionSelection{VersionTag: *calledElement.VersionTag}, nil
	case "deployment":
		// bindingType existed before call-activity version selection and was
		// previously ignored. Preserve latest-version behavior for stored models.
		return VersionSelection{}, nil
	default:
		// Unknown legacy binding values were also ignored, and therefore meant
		// latest. Keeping that behavior prevents existing definitions from
		// becoming unreadable after an upgrade.
		return VersionSelection{}, nil
	}
}
