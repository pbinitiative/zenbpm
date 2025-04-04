package bpmn

// import (
// 	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
// 	"time"
// )
//
// // FIXME: shall this be exported?
// type processInstanceInfo struct {
// 	ProcessInfo    *runtime.ProcessDefinition `json:"-"`
// 	InstanceKey    int64                      `json:"ik"`
// 	VariableHolder runtime.VariableHolder     `json:"vh,omitempty"`
// 	CreatedAt      time.Time                  `json:"c"`
// 	State          runtime.ActivityState      `json:"s"`
// 	CaughtEvents   []catchEvent               `json:"ce,omitempty"`
// 	activities     []runtime.Activity
// }
//
// func (pii *processInstanceInfo) GetProcessInfo() *runtime.ProcessDefinition {
// 	return pii.ProcessInfo
// }
//
// func (pii *processInstanceInfo) GetInstanceKey() int64 {
// 	return pii.InstanceKey
// }
//
// func (pii *processInstanceInfo) GetVariable(key string) interface{} {
// 	return pii.VariableHolder.GetVariable(key)
// }
//
// func (pii *processInstanceInfo) SetVariable(key string, value interface{}) {
// 	pii.VariableHolder.SetVariable(key, value)
// }
//
// func (pii *processInstanceInfo) GetCreatedAt() time.Time {
// 	return pii.CreatedAt
// }
//
// // GetState returns one of [ Ready, Active, Completed, Failed ]
// func (pii *processInstanceInfo) GetState() runtime.ActivityState {
// 	return pii.State
// }
//
// func (pii *processInstanceInfo) appendActivity(activity runtime.Activity) {
// 	pii.activities = append(pii.activities, activity)
// }
//
// func (pii *processInstanceInfo) findActiveActivityByElementId(id string) runtime.Activity {
// 	for _, a := range pii.activities {
// 		if a.Element().GetId() == id && a.State() == runtime.Active {
// 			return a
// 		}
// 	}
// 	return nil
// }
//
// func (pii *processInstanceInfo) findActivity(key int64) runtime.Activity {
// 	for _, a := range pii.activities {
// 		if a.Key() == key {
// 			return a
// 		}
// 	}
// 	return nil
// }
