package bpmn

func (engine *Engine) generateKey() int64 {
	return engine.persistence.GenerateId()
}
