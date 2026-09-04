package bpmn

import (
	"context"
	"crypto/md5"
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
	md5sum := md5.Sum(xmlData)
	var definitions bpmn20.TDefinitions
	err := xml.Unmarshal(xmlData, &definitions)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal xml data: %w", err)
	}

	versionTag, err := extractProcessVersionTag(xmlData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse process version tag: %w", err)
	}

	processInfo := runtime.ProcessDefinition{
		Version:         1,
		BpmnProcessId:   definitions.Process.Id,
		BpmnProcessName: definitions.Process.Name,
		Key:             key,
		Definitions:     definitions,
		BpmnData:        string(xmlData),
		BpmnChecksum:    md5sum,
		VersionTag:      versionTag,
	}
	processes, err := engine.persistence.FindProcessDefinitionsById(ctx, definitions.Process.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to load processes by id %s: %w", definitions.Process.Id, err)
	}
	if len(processes) > 0 {
		latest := &processes[0]
		for i := range processes {
			if latest.Version < processes[i].Version {
				latest = &processes[i]
			}
		}
		sameContent, err := xmlutil.SameContent(
			latest.BpmnChecksum[:],
			md5sum[:],
			[]byte(latest.BpmnData),
			xmlData,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to compare BPMN content for process %s: %w", definitions.Process.Id, err)
		}
		if sameContent {
			return latest, nil
		}
		for i := range processes {
			if processes[i].VersionTag == versionTag && versionTag != "" {
				return nil, fmt.Errorf("process definition with id %q and version tag %q already exists: %w", definitions.Process.Id, versionTag, storage.ErrUniqueConstraint)
			}
		}
		if err := engine.deleteProcessDefinitionSubscriptions(ctx, latest); err != nil {
			return nil, err
		}
		processInfo.Version = latest.Version + 1
	}
	err = engine.persistence.SaveProcessDefinition(ctx, processInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to save process definition: %w", err)
	}

	engine.exportNewProcessEvent(processInfo, xmlData, hex.EncodeToString(md5sum[:]))
	return &processInfo, nil
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
