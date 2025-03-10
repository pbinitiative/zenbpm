package dmn

import "fmt"

type DecisionNotFoundError struct {
	DecisionID string
	Message    string
}

func (e *DecisionNotFoundError) Error() string {
	return fmt.Sprintf("Decision ID [%v] doesnt exist.", e.DecisionID)
}
