package bpmn

import (
	"context"
	"crypto/md5" // #nosec G501 -- MD5 is a content fingerprint for change detection, not a security primitive
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"os"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/xmlutil"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

// LoadFromFile loads a given BPMN file by filename into the engine
// and returns ProcessInfo details for the deployed workflow
func (engine *Engine) LoadFromFile(ctx context.Context, filename string) (*runtime.ProcessDefinition, error) {
	xmlData, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to load from file: %w", err)
	}
	return engine.load(ctx, xmlData, engine.generateKey())
}

// LoadFromBytes loads a given BPMN file by xmlData byte array into the engine
// and returns ProcessInfo details for the deployed workflow
func (engine *Engine) LoadFromBytes(ctx context.Context, xmlData []byte, key int64) (*runtime.ProcessDefinition, error) {
	def, err := engine.load(ctx, xmlData, key)
	if err != nil {
		return nil, fmt.Errorf("failed to load from bytes: %w", err)
	}
	return def, nil
}

func (engine *Engine) load(ctx context.Context, xmlData []byte, key int64) (*runtime.ProcessDefinition, error) {
	processInfo, err := parseProcessDefinition(xmlData, key)
	if err != nil {
		return nil, err
	}
	definitions := processInfo.Definitions
	engine.definitionMu.Lock()
	defer engine.definitionMu.Unlock()
	processes, err := engine.persistence.FindProcessDefinitionsById(ctx, definitions.Process.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to load processes by id %s: %w", definitions.Process.Id, err)
	}
	if latest := latestProcessDefinition(processes); latest != nil {
		sameContent, err := xmlutil.SameContent(
			latest.BpmnChecksum[:],
			processInfo.BpmnChecksum[:],
			[]byte(latest.BpmnData),
			xmlData,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to compare BPMN content for process %s: %w", definitions.Process.Id, err)
		}
		if sameContent {
			return latest, nil
		}
		processInfo.Version = latest.Version + 1
	}
	stored, err := engine.storeProcessDefinitionVersion(ctx, processInfo, processes, true)
	if err != nil {
		return nil, err
	}
	return stored, nil
}

// DeployProcessDefinition stores xmlData under the key and version the
// cluster allocated for this deployment. Unlike LoadFromBytes the partition
// neither deduplicates content nor assigns a version: the cluster decided
// both once, so every partition ends up with the same (process id, version)
// → definition mapping regardless of the order deployments arrive in. The
// call is idempotent: a definition already stored under key is returned as is,
// which makes retries after partial failures and leader changes safe.
//
// A different definition of the same process that already holds the version
// (or the version tag) means the partition diverged from the cluster
// allocation; the deployment is refused with storage.ErrUniqueConstraint and
// nothing is written.
//
// When the deployed version becomes the newest one of its process, the
// definition-level subscriptions of the previously newest version are
// retired, as any deployment does. Registering the subscriptions of the new
// version is left to the caller (only one partition owns them).
func (engine *Engine) DeployProcessDefinition(ctx context.Context, xmlData []byte, key int64, version int32) (*runtime.ProcessDefinition, error) {
	if version < 1 {
		return nil, fmt.Errorf("failed to deploy process definition %d: version must be positive, got %d", key, version)
	}
	processInfo, err := parseProcessDefinition(xmlData, key)
	if err != nil {
		return nil, fmt.Errorf("failed to deploy process definition %d: %w", key, err)
	}
	processInfo.Version = version
	engine.definitionMu.Lock()
	defer engine.definitionMu.Unlock()
	processes, err := engine.persistence.FindProcessDefinitionsById(ctx, processInfo.BpmnProcessId)
	if err != nil {
		return nil, fmt.Errorf("failed to load processes by id %s: %w", processInfo.BpmnProcessId, err)
	}
	for i := range processes {
		if processes[i].Key == key {
			return &processes[i], nil
		}
	}
	return engine.storeProcessDefinitionVersion(ctx, processInfo, processes, true)
}

// storeProcessDefinitionVersion saves definition under the key and version it
// carries, next to the existing definitions of the same process. Another
// definition holding the same version or version tag is refused with
// storage.ErrUniqueConstraint before anything is written. When the definition
// becomes the newest version of its process the definition-level
// subscriptions of the previously newest one are retired first. With
// exportEvent the deployment is reported to the exporters.
func (engine *Engine) storeProcessDefinitionVersion(ctx context.Context, definition runtime.ProcessDefinition, existing []runtime.ProcessDefinition, exportEvent bool) (*runtime.ProcessDefinition, error) {
	for i := range existing {
		other := &existing[i]
		if other.Version == definition.Version {
			return nil, fmt.Errorf("process definition %d cannot be stored as version %d of %q: definition %d already holds that version: %w",
				definition.Key, definition.Version, definition.BpmnProcessId, other.Key, storage.ErrUniqueConstraint)
		}
		if definition.VersionTag != "" && other.VersionTag == definition.VersionTag {
			return nil, fmt.Errorf("process definition with id %q and version tag %q already exists: %w", definition.BpmnProcessId, definition.VersionTag, storage.ErrUniqueConstraint)
		}
	}
	if previousLatest := latestProcessDefinition(existing); previousLatest != nil && previousLatest.Version < definition.Version {
		if err := engine.deleteProcessDefinitionSubscriptions(ctx, previousLatest); err != nil {
			return nil, err
		}
	}
	if err := engine.persistence.SaveProcessDefinition(ctx, definition); err != nil {
		return nil, fmt.Errorf("failed to save process definition: %w", err)
	}
	if exportEvent {
		engine.exportNewProcessEvent(definition, []byte(definition.BpmnData), hex.EncodeToString(definition.BpmnChecksum[:]))
	}
	return &definition, nil
}

// latestProcessDefinition returns the definition with the highest version,
// nil when there is none.
func latestProcessDefinition(definitions []runtime.ProcessDefinition) *runtime.ProcessDefinition {
	var latest *runtime.ProcessDefinition
	for i := range definitions {
		if latest == nil || latest.Version < definitions[i].Version {
			latest = &definitions[i]
		}
	}
	return latest
}

// ProcessDefinitionIdentity is what identifies a BPMN resource before it is
// deployed: the process id it declares, its version tag and the fingerprint
// of its raw bytes.
type ProcessDefinitionIdentity struct {
	ProcessId  string
	VersionTag string
	// Checksum is the MD5 of the raw BPMN bytes, the same fingerprint a
	// deployed definition stores.
	Checksum [16]byte
}

// ParseProcessDefinitionIdentity reads the identity of a BPMN resource
// without touching storage.
func ParseProcessDefinitionIdentity(xmlData []byte) (ProcessDefinitionIdentity, error) {
	definition, err := parseProcessDefinition(xmlData, 0)
	if err != nil {
		return ProcessDefinitionIdentity{}, err
	}
	return ProcessDefinitionIdentity{
		ProcessId:  definition.BpmnProcessId,
		VersionTag: definition.VersionTag,
		Checksum:   definition.BpmnChecksum,
	}, nil
}

// parseProcessDefinition builds the definition record for xmlData under the
// given key without touching storage. The version starts at 1; the caller
// decides whether to assign the next one (deployment) or keep a given one
// (import).
func parseProcessDefinition(xmlData []byte, key int64) (runtime.ProcessDefinition, error) {
	var definitions bpmn20.TDefinitions
	if err := xml.Unmarshal(xmlData, &definitions); err != nil {
		return runtime.ProcessDefinition{}, fmt.Errorf("failed to unmarshal xml data: %w", err)
	}
	versionTag, err := extractProcessVersionTag(xmlData)
	if err != nil {
		return runtime.ProcessDefinition{}, fmt.Errorf("failed to parse process version tag: %w", err)
	}
	return runtime.ProcessDefinition{
		Version:         1,
		BpmnProcessId:   definitions.Process.Id,
		BpmnProcessName: definitions.Process.Name,
		Key:             key,
		Definitions:     definitions,
		BpmnData:        string(xmlData),
		BpmnChecksum:    md5.Sum(xmlData), // #nosec G401 -- MD5 is a content fingerprint for change detection, not a security primitive
		VersionTag:      versionTag,
	}, nil
}

type processVersionTagDefinitions struct {
	Process processVersionTagProcess `xml:"process"`
}

type processVersionTagProcess struct {
	ExtensionElements processVersionTagExtensionElements `xml:"extensionElements"`
}

type processVersionTagExtensionElements struct {
	VersionTag processVersionTagElement `xml:"versionTag"`
}

type processVersionTagElement struct {
	Value string `xml:"value,attr"`
}

func extractProcessVersionTag(xmlData []byte) (string, error) {
	var definitions processVersionTagDefinitions
	if err := xml.Unmarshal(xmlData, &definitions); err != nil {
		return "", err
	}
	return definitions.Process.ExtensionElements.VersionTag.Value, nil
}

func (engine *Engine) deleteProcessDefinitionSubscriptions(ctx context.Context, latest *runtime.ProcessDefinition) error {
	if err := engine.persistence.DeleteProcessDefinitionsTimers(ctx, []int64{latest.Key}); err != nil {
		return fmt.Errorf("failed to delete process definitions timers: %w", err)
	}
	if err := engine.persistence.DeleteProcessDefinitionsMessageSubscriptions(ctx, []int64{latest.Key}); err != nil {
		return fmt.Errorf("failed to delete process definitions message subscriptions: %w", err)
	}
	return nil
}
