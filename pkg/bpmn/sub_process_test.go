package bpmn

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func TestCallActivityStartsAndCompletes(t *testing.T) {
	// setup
	_, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile("./test-cases/call-activity-simple.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	taskId := "id"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	handler := func(job ActivatedJob) {
		v := job.Variable(variableName)
		assert.Equal(t, "oldVal", v, "one should be able to read variables")
		job.SetOutputVariable(variableName, "newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	// given
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, instance, "Process instance needs to be present")
	assert.Equal(t, "newVal", instance.ProcessInstance().VariableHolder.GetLocalVariable(variableName))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
}

func TestCallActivityStartsAndCompletesAfterFinishingTheJob(t *testing.T) {
	// setup
	_, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile("./test-cases/call-activity-simple.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	// wait for call activity process to be created
	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var foundInstance runtime.CallActivityInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.Type() != runtime.ProcessTypeCallActivity {
			continue
		}
		if pi.(*runtime.CallActivityInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundInstance = *pi.(*runtime.CallActivityInstance)
			break
		}
	}

	var job runtime.Job
	jobs, err := bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), foundInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs), "There should be one job")
	job = jobs[0]

	assert.NoError(t, err)
	bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
		variableName: "newVal",
	})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	v, err := bpmnEngine.FindProcessInstance(foundInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, v, "Process instance needs to be present")
	assert.Equal(t, runtime.ActivityStateCompleted.String(), v.ProcessInstance().State.String())
	assert.Equal(t, "newVal", v.ProcessInstance().VariableHolder.GetLocalVariable(variableName))
}

func TestCallActivityCancelsOnInterruptingBoundaryEvent(t *testing.T) {
	// setup
	_, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile("./test-cases/call-activity-with-boundary-simple.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 2)
	variableContext[variableName] = "oldVal"

	randomCorellationKey := rand.Int63()

	variableContext["correlationKey"] = fmt.Sprint(randomCorellationKey)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	// wait for call activity process to be created
	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var foundChildInstance runtime.CallActivityInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.Type() != runtime.ProcessTypeCallActivity {
			continue
		}
		if pi.(*runtime.CallActivityInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundChildInstance = *pi.(*runtime.CallActivityInstance)
			break
		}
	}

	// when
	variables := map[string]interface{}{"payload": "message payload"}
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", fmt.Sprint(randomCorellationKey), variables)
	assert.NoError(t, err)

	// then
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState(), "Parent instance should be completed")

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), foundChildInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, instance.ProcessInstance().GetState(), "Child instance should be terminated")

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "TestType")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
}

func TestSubProcessStartsAndCompletes(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/simple_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	taskId := "id"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	handler := func(job ActivatedJob) {
		v := job.Variable("testInput")
		assert.Equal(t, "oldVal", v, "one should be able to read variables")
		job.SetOutputVariable("testJobOutput", "newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	// given
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, instance, "Process instance needs to be present")
	assert.Equal(t, "newVal", instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutput"))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
}

func TestMultiInstanceServiceTaskStartsAndCompletesLocalJob(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi_instance_service_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "Activity_0rae016"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	// given
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	// when
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceServiceTaskStartsAndCompletesOnWorkerJob(t *testing.T) {
	//TODO: Some test in another file is leaving jobs behind
	engineStorage.Jobs = make(map[int64]runtime.Job)

	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi_instance_service_task.bpmn")
	assert.NoError(t, err)

	// when
	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []string{"test1", "test2", "test3"}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1000 * time.Millisecond)

	variableName := "testJobOutput"
	for _, str := range testInputCollection {
		var job runtime.Job
		jobs, err := bpmnEngine.persistence.FindActiveJobsByType(t.Context(), "TestType")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(jobs), 1, "There should be one job")
		job = jobs[0]

		err = bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
			variableName: str + "newVal",
		})
		assert.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceParallelServiceTaskStartsAndCompletesWorkerJob(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi_instance_parallel_service_task.bpmn")
	assert.NoError(t, err)

	// when
	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []string{"test1", "test2", "test3"}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1000 * time.Millisecond)

	variableName := "testJobOutput"
	for _, str := range testInputCollection {
		var job runtime.Job
		jobs, err := bpmnEngine.persistence.FindActiveJobsByType(t.Context(), "TestType")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobs, "There should be at least one job")
		job = jobs[0]

		err = bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
			variableName: str + "newVal",
		})
		assert.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceParallelServiceTaskStartsAndCompletesLocalJob(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi_instance_parallel_service_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "Activity_0rae016"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	// given
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	// when
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceBusinessRuleTaskStartsAndCompletesLocalJob(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi_instance_business_rule.bpmn")
	assert.NoError(t, err)

	definition, xmldata, err := bpmnEngine.dmnEngine.ParseDmnFromFile(filepath.Join("..", "dmn", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	assert.NoError(t, err)
	_, _, err = bpmnEngine.dmnEngine.SaveDmnResourceDefinition(
		t.Context(),
		definition,
		xmldata,
		bpmnEngine.generateKey(),
	)
	assert.NoError(t, err)

	// when
	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []int{1000, 500000, 200}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []map[string]interface{}{{"canAutoLiquidate": true}, {"canAutoLiquidate": false}, {"canAutoLiquidate": true}}, instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection"))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceParallelBusinessRuleTaskStartsAndCompletesLocalJob(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi_instance_parallel_business_rule.bpmn")
	assert.NoError(t, err)

	definition, xmldata, err := bpmnEngine.dmnEngine.ParseDmnFromFile(filepath.Join("..", "dmn", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	assert.NoError(t, err)
	_, _, err = bpmnEngine.dmnEngine.SaveDmnResourceDefinition(
		t.Context(),
		definition,
		xmldata,
		bpmnEngine.generateKey(),
	)
	assert.NoError(t, err)

	// when
	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []int{1000, 500000, 200}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []map[string]interface{}{{"canAutoLiquidate": true}, {"canAutoLiquidate": false}, {"canAutoLiquidate": true}}, instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection"))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceCallActivityStartsAndCompletes(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi_instance_call_activity_process.bpmn")
	assert.NoError(t, err)

	process, err = bpmnEngine.LoadFromFile("./test-cases/multi_instance_call_activity_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "id"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	// given
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	// when
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceParallelCallActivityStartsAndCompletes(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi_instance_call_activity_process.bpmn")
	assert.NoError(t, err)

	process, err = bpmnEngine.LoadFromFile("./test-cases/multi_instance_parallel_call_activity_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "id"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	// given
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	// when
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 5000*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))

	assert.Eventually(t, func() bool {
		subprocess, testErr := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), subProcesses[0].ProcessInstance().Key)
		assert.NoError(t, testErr)
		if subprocess.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 5000*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceSubProcessStartsAndCompletes(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi_instance_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "id"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	// given
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	// when
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceParallelSubProcessStartsAndCompletes(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/multi_instance_parallel_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "id"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	// given
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	// when
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 5000*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))

	assert.Eventually(t, func() bool {
		subprocess, testErr := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), subProcesses[0].ProcessInstance().Key)
		assert.NoError(t, testErr)
		if subprocess.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 5000*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}
