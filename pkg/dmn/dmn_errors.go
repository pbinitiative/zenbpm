package dmn

import "fmt"

type DecisionNotFoundError struct {
	DecisionID string
	Message    string
}

func (e *DecisionNotFoundError) Error() string {
	return fmt.Sprintf("Decision ID [%v] doesnt exist.", e.DecisionID)
}

type DmnEngineUnmarshallingError struct {
	Msg string
	Err error
}

func (e *DmnEngineUnmarshallingError) Error() string {
	if len(e.Msg) > 0 {
		return e.Msg + ": " + e.Err.Error()
	}
	return e.Err.Error()
}
