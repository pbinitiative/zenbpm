package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	ssql "database/sql"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/profile"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/store"
)

type RqLiteDB struct {
	store   *store.Store
	queries *sql.Queries
	logger  hclog.Logger
}

func NewRqLiteDB(store *store.Store, logger hclog.Logger) *RqLiteDB {
	db := &RqLiteDB{
		store:  store,
		logger: logger,
	}
	queries := sql.New(db)
	db.queries = queries
	return db
}

func (rq *RqLiteDB) executeStatements(ctx context.Context, statements []*proto.Statement) ([]*proto.ExecuteQueryResponse, error) {
	er := &proto.ExecuteRequest{
		Request: &proto.Request{
			Transaction: true,
			DbTimeout:   int64(0),
			Statements:  statements,
		},
		Timings: false,
	}

	results, resultsErr := rq.store.Execute(er)

	if resultsErr != nil {
		if ctx.Err() == context.DeadlineExceeded {
			rq.logger.Error("Context deadline exceeded for statement", "err", resultsErr)
		}
		rq.logger.Error("Error executing SQL statements", "err", resultsErr)
		return nil, resultsErr
	}
	return results, nil
}

func (rq *RqLiteDB) generateStatement(sql string, parameters ...interface{}) *proto.Statement {
	resultParams := make([]*proto.Parameter, 0)

	for _, par := range parameters {
		switch par := par.(type) {
		case string:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_S{
					S: par,
				},
			})
		case int64:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_I{
					I: par,
				},
			})
		case int32:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_I{
					I: int64(par),
				},
			})
		case int:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_I{
					I: int64(par),
				},
			})
		case []int:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_S{
					S: strings.Trim(strings.Join(strings.Fields(fmt.Sprint(par)), ","), "[]"),
				},
			})
		case float64:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_D{
					D: par,
				},
			})
		case bool:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_B{
					B: par,
				},
			})
		case []byte:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_Y{
					Y: par,
				},
			})
		case ssql.NullInt64:
			if par.Valid {
				resultParams = append(resultParams, &proto.Parameter{
					Value: &proto.Parameter_I{
						I: par.Int64,
					},
				})
			} else {
				resultParams = append(resultParams, &proto.Parameter{})
			}
		case ssql.NullString:
			if par.Valid {
				resultParams = append(resultParams, &proto.Parameter{
					Value: &proto.Parameter_S{
						S: par.String,
					},
				})
			} else {
				resultParams = append(resultParams, &proto.Parameter{})
			}
		default:
			rq.logger.Error(fmt.Sprintf("Unknown parameter type: %T", par))
			if profile.Current == profile.DEV || profile.Current == profile.TEST {
				panic(fmt.Sprintf("Unknown parameter type: %T", par))
			}
		}

	}
	return &proto.Statement{
		Sql:        sql,
		Parameters: resultParams,
	}
}

func (rq *RqLiteDB) queryDatabase(query string, parameters ...interface{}) ([]*proto.QueryRows, error) {

	stmts := rq.generateStatement(query, parameters...)

	qr := &proto.QueryRequest{
		Request: &proto.Request{
			Transaction: false,
			DbTimeout:   int64(0),
			Statements:  []*proto.Statement{stmts},
		},
		Timings: false,
		Level:   proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE,
		// TODO: this needs to be revised
		Freshness:       1000000000,
		FreshnessStrict: false,
	}

	results, resultsErr := rq.store.Query(qr)
	if resultsErr != nil {
		rq.logger.Error("Error executing SQL statements", "err", resultsErr)
		return nil, resultsErr
	}
	return results, nil
}

type rqliteResult struct {
	lastInsertId int64
	rowsAffected int64
}

func (r rqliteResult) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r rqliteResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func (rq *RqLiteDB) ExecContext(ctx context.Context, sql string, args ...interface{}) (ssql.Result, error) {
	result, err := rq.executeStatements(ctx, []*proto.Statement{rq.generateStatement(sql, args...)})

	if err != nil {
		rq.logger.Error("Error executing SQL statements")
		return nil, err
	}

	lastInsertId, rowsAffected := int64(-1), int64(-1)
	for _, r := range result {
		err := r.GetError()
		if err != "" {
			return nil, errors.New(err)
		}
		lastInsertId, rowsAffected = r.GetE().LastInsertId, r.GetE().RowsAffected+rowsAffected
	}
	return rqliteResult{lastInsertId: lastInsertId, rowsAffected: rowsAffected}, nil
}

func (rq *RqLiteDB) PrepareContext(ctx context.Context, sql string) (*ssql.Stmt, error) {
	return nil, errors.New("PrepareContext not supported by rqlite")
}

func (rq *RqLiteDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	results, err := rq.queryDatabase(query, args...)
	if err != nil {
		return nil, err
	}
	if len(results) > 1 {
		return nil, errors.New("Multiple results not supported")
	}
	for _, r := range results {
		return sql.ConstructRows(ctx, r.Columns, r.Types, r.Values), nil
	}
	// empty results
	return sql.ConstructRows(ctx, []string{}, []string{}, []*proto.Values{}), nil
}

func (rq *RqLiteDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	rows, err := rq.QueryContext(ctx, query, args...)
	if err != nil {
		return sql.ConstructRow(ctx, []string{}, []string{}, nil, err)
	}
	defer rows.Close()

	row := rows.Next()
	if !row {

		return sql.ConstructRow(ctx, []string{}, []string{}, nil, errors.New("No rows"))
	} else {
		return sql.ConstructRowFromRows(ctx, rows, nil)
	}
}

var _ storage.Storage = &RqLiteDB{}

func (rq *RqLiteDB) NewBatch() storage.Batch {
	batch := &RqLiteDBBatch{
		db:        rq,
		stmtToRun: make([]*proto.Statement, 0, 10),
	}
	queries := sql.New(batch)
	batch.queries = queries
	return batch
}

var _ storage.ProcessDefinitionStorageReader = &RqLiteDB{}

func (rq *RqLiteDB) FindLatestProcessDefinitionById(ctx context.Context, processDefinitionId string) (runtime.ProcessDefinition, error) {
	var res runtime.ProcessDefinition
	dbDefinition, err := rq.queries.FindLatestProcessDefinitionById(ctx, processDefinitionId)
	if err != nil {
		return res, fmt.Errorf("failed to find latest process definition: %w", err)
	}

	res = runtime.ProcessDefinition{
		BpmnProcessId: dbDefinition.BpmnProcessID,
		Version:       dbDefinition.Version,
		Key:           dbDefinition.Key,
		// Definitions:      bpmn20.TDefinitions{}, //TODO: do we initialize somehow?
		BpmnData:         dbDefinition.BpmnData,
		BpmnResourceName: dbDefinition.BpmnResourceName,
		BpmnChecksum:     [16]byte(dbDefinition.BpmnChecksum),
	}

	return res, nil
}

func (rq *RqLiteDB) FindProcessDefinitionByKey(ctx context.Context, processDefinitionKey int64) (runtime.ProcessDefinition, error) {
	var res runtime.ProcessDefinition
	dbDefinition, err := rq.queries.FindProcessDefinitionByKey(ctx, processDefinitionKey)
	if err != nil {
		return res, fmt.Errorf("failed to find latest process definition: %w", err)
	}

	res = runtime.ProcessDefinition{
		BpmnProcessId: dbDefinition.BpmnProcessID,
		Version:       dbDefinition.Version,
		Key:           dbDefinition.Key,
		// Definitions:      bpmn20.TDefinitions{}, //TODO: do we initialize somehow?
		BpmnData:         dbDefinition.BpmnData,
		BpmnResourceName: dbDefinition.BpmnResourceName,
		BpmnChecksum:     [16]byte(dbDefinition.BpmnChecksum),
	}
	return res, nil
}

func (rq *RqLiteDB) FindProcessDefinitionsById(ctx context.Context, processId string) ([]runtime.ProcessDefinition, error) {
	dbDefinitions, err := rq.queries.FindProcessDefinitionsByIds(ctx, processId)
	if err != nil {
		return nil, fmt.Errorf("failed to find process definitions by id: %w", err)
	}

	res := make([]runtime.ProcessDefinition, len(dbDefinitions))
	for i, def := range dbDefinitions {
		res[i] = runtime.ProcessDefinition{
			BpmnProcessId: def.BpmnProcessID,
			Version:       def.Version,
			Key:           def.Key,
			// Definitions:      bpmn20.TDefinitions{}, //TODO: do we initialize somehow?
			BpmnData:         def.BpmnData,
			BpmnResourceName: def.BpmnResourceName,
			BpmnChecksum:     [16]byte(def.BpmnChecksum),
		}
	}
	return res, nil
}

var _ storage.ProcessDefinitionStorageWriter = &RqLiteDB{}

func (rq *RqLiteDB) SaveProcessDefinition(ctx context.Context, definition runtime.ProcessDefinition) error {
	return SaveProcessDefinitionWith(ctx, rq.queries, definition)
}

func SaveProcessDefinitionWith(ctx context.Context, db *sql.Queries, definition runtime.ProcessDefinition) error {
	err := db.SaveProcessDefinition(ctx, sql.SaveProcessDefinitionParams{
		Key:              definition.Key,
		Version:          definition.Version,
		BpmnProcessID:    definition.BpmnProcessId,
		BpmnData:         definition.BpmnData,
		BpmnChecksum:     definition.BpmnChecksum[:],
		BpmnResourceName: definition.BpmnResourceName,
	})
	if err != nil {
		return fmt.Errorf("failed to save process definition: %w", err)
	}
	return nil
}

var _ storage.ProcessInstanceStorageReader = &RqLiteDB{}

func (rq *RqLiteDB) FindProcessInstanceByKey(ctx context.Context, processInstanceKey int64) (runtime.ProcessInstance, error) {
	var res runtime.ProcessInstance
	dbInstance, err := rq.queries.GetProcessInstance(ctx, processInstanceKey)
	if err != nil {
		return res, fmt.Errorf("failed to find process instance by key: %w", err)
	}

	variables := map[string]any{}
	err = json.Unmarshal([]byte(dbInstance.Variables), &variables)
	if err != nil {
		return res, fmt.Errorf("failed to unmarshal variables: %w", err)
	}

	// TODO: load all activities from DB
	// dbActivities, err := rq.queries.FindActivityInstances(ctx, dbInstance.Key)
	// if err != nil {
	// 	return res, fmt.Errorf("failed to find activities for process instance key (%d): %w", dbInstance.Key, err)
	// }

	res = runtime.ProcessInstance{
		// Definition:     &runtime.ProcessDefinition{}, //TODO: load from cache
		Key:            dbInstance.Key,
		VariableHolder: runtime.NewVariableHolder(nil, variables),
		CreatedAt:      time.UnixMilli(dbInstance.CreatedAt),
		State:          runtime.ActivityState(dbInstance.State),
		// CaughtEvents:   []runtime.CatchEvent{}, //TODO: do something
		// Activities: make([]runtime.Activity, len(dbActivities)),
	}

	return res, nil
}

var _ storage.ProcessInstanceStorageWriter = &RqLiteDB{}

func (rq *RqLiteDB) SaveProcessInstance(ctx context.Context, processInstance runtime.ProcessInstance) error {
	return SaveProcessInstanceWith(ctx, rq.queries, processInstance)
}

func SaveProcessInstanceWith(ctx context.Context, db *sql.Queries, processInstance runtime.ProcessInstance) error {
	varStr, err := json.Marshal(processInstance.VariableHolder.Variables())
	if err != nil {
		return fmt.Errorf("failed to marshal variables for instance %d: %w", processInstance.Key, err)
	}
	err = db.SaveProcessInstance(ctx, sql.SaveProcessInstanceParams{
		Key:                  processInstance.Key,
		ProcessDefinitionKey: processInstance.Definition.Key,
		CreatedAt:            processInstance.CreatedAt.UnixMilli(),
		State:                int(processInstance.State),
		Variables:            string(varStr),
		// CaughtEvents:         "",
		// Activities:           , //TODO: what do we save here? we have activity_instance table
	})
	if err != nil {
		return fmt.Errorf("failed to save process instance %d: %w", processInstance.Key, err)
	}
	return nil
}

var _ storage.TimerStorageReader = &RqLiteDB{}

func (rq *RqLiteDB) FindActivityTimers(ctx context.Context, activityKey int64, state runtime.TimerState) ([]runtime.Timer, error) {
	dbTimers, err := rq.queries.FindElementTimers(ctx, sql.FindElementTimersParams{
		ElementInstanceKey: activityKey,
		State:              int(state),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find element timers %d: %w", activityKey, err)
	}
	res := make([]runtime.Timer, len(dbTimers))
	for i, timer := range dbTimers {
		res[i] = runtime.Timer{
			ElementId:            timer.ElementID,
			Key:                  timer.ElementInstanceKey,
			ProcessDefinitionKey: timer.ProcessDefinitionKey,
			ProcessInstanceKey:   timer.ProcessInstanceKey,
			TimerState:           runtime.TimerState(timer.State),
			CreatedAt:            time.UnixMilli(timer.CreatedAt),
			DueAt:                time.UnixMilli(timer.DueAt),
			// OriginActivity:     timer.ElementID, // TODO: load process from cache and find its activity by id
			// BaseElement:        nil,
		}
		res[i].Duration = res[i].DueAt.Sub(res[i].CreatedAt)
	}
	return res, nil
}

func (rq *RqLiteDB) FindTimersByState(ctx context.Context, processInstanceKey int64, state runtime.TimerState) ([]runtime.Timer, error) {
	dbTimers, err := rq.queries.FindTimersInState(ctx, sql.FindTimersInStateParams{
		ProcessInstanceKey: processInstanceKey,
		State:              int(state),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find process instance timers %d: %w", processInstanceKey, err)
	}
	res := make([]runtime.Timer, len(dbTimers))
	for i, timer := range dbTimers {
		res[i] = runtime.Timer{
			ElementId:            timer.ElementID,
			Key:                  timer.ElementInstanceKey,
			ProcessDefinitionKey: timer.ProcessDefinitionKey,
			ProcessInstanceKey:   timer.ProcessInstanceKey,
			TimerState:           runtime.TimerState(timer.State),
			CreatedAt:            time.UnixMilli(timer.CreatedAt),
			DueAt:                time.UnixMilli(timer.DueAt),
			Duration:             time.Millisecond * time.Duration(timer.DueAt-timer.CreatedAt),
			// OriginActivity:     timer.ElementID, // TODO: load process from cache and find its activity by id
			// BaseElement:        nil,
		}
	}
	return res, nil
}

var _ storage.TimerStorageWriter = &RqLiteDB{}

func (rq *RqLiteDB) SaveTimer(ctx context.Context, timer runtime.Timer) error {
	return SaveTimerWith(ctx, rq.queries, timer)
}

func SaveTimerWith(ctx context.Context, db *sql.Queries, timer runtime.Timer) error {
	err := db.SaveTimer(ctx, sql.SaveTimerParams{
		Key:                  timer.GetKey(),
		ElementID:            timer.ElementId,
		ElementInstanceKey:   timer.Key,
		ProcessDefinitionKey: timer.ProcessDefinitionKey,
		ProcessInstanceKey:   timer.ProcessInstanceKey,
		State:                int(timer.GetState()),
		CreatedAt:            timer.CreatedAt.UnixMilli(),
		DueAt:                timer.DueAt.UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("failed to save timer %d: %w", timer.GetKey(), err)
	}
	return nil
}

var _ storage.JobStorageReader = &RqLiteDB{}

func (rq *RqLiteDB) FindActiveJobsByType(ctx context.Context, jobType string) ([]runtime.Job, error) {
	jobs, err := rq.queries.FindActiveJobsByType(ctx, jobType)
	if err != nil {
		return nil, fmt.Errorf("failed to find active jobs for type %s: %w", jobType, err)
	}
	res := make([]runtime.Job, len(jobs))
	for i, job := range jobs {
		res[i] = runtime.Job{
			ElementId:          job.ElementID,
			ElementInstanceKey: job.ElementInstanceKey,
			ProcessInstanceKey: job.ProcessInstanceKey,
			Key:                job.Key,
			State:              runtime.ActivityState(job.State),
			CreatedAt:          time.UnixMilli(job.CreatedAt),
			// BaseElement:        ,
		}
	}
	return res, nil
}

func (rq *RqLiteDB) FindJobByElementID(ctx context.Context, processInstanceKey int64, elementID string) (runtime.Job, error) {
	var res runtime.Job
	job, err := rq.queries.FindJobByElementId(ctx, sql.FindJobByElementIdParams{
		ElementID:          elementID,
		ProcessInstanceKey: processInstanceKey,
	})
	if err != nil {
		return res, fmt.Errorf("failed to find job for elementId %s and process instance key %d: %w", elementID, processInstanceKey, err)
	}
	res = runtime.Job{
		ElementId:          job.ElementID,
		ElementInstanceKey: job.ElementInstanceKey,
		ProcessInstanceKey: job.ProcessInstanceKey,
		Key:                job.Key,
		State:              runtime.ActivityState(job.State),
		CreatedAt:          time.UnixMilli(job.CreatedAt),
		// BaseElement:        nil,
	}
	return res, nil
}

func (rq *RqLiteDB) FindJobByJobKey(ctx context.Context, jobKey int64) (runtime.Job, error) {
	var res runtime.Job
	job, err := rq.queries.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return res, fmt.Errorf("failed to find job with key %d: %w", jobKey, err)
	}
	res = runtime.Job{
		ElementId:          job.ElementID,
		ElementInstanceKey: job.ElementInstanceKey,
		ProcessInstanceKey: job.ProcessInstanceKey,
		Key:                job.Key,
		State:              runtime.ActivityState(job.State),
		CreatedAt:          time.UnixMilli(job.CreatedAt),
		// BaseElement:        nil,
	}
	return res, nil
}

func (rq *RqLiteDB) FindPendingProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]runtime.Job, error) {
	dbJobs, err := rq.queries.FindProcessInstanceJobsInState(ctx, sql.FindProcessInstanceJobsInStateParams{
		ProcessInstanceKey: processInstanceKey,
		States:             []int{int(runtime.ActivityStateCompleting), int(runtime.ActivityStateActive)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find pending process instance jobs for process instance key %d: %w", processInstanceKey, err)
	}
	res := make([]runtime.Job, len(dbJobs))
	for i, job := range dbJobs {
		res[i] = runtime.Job{
			ElementId:          job.ElementID,
			ElementInstanceKey: job.ElementInstanceKey,
			ProcessInstanceKey: job.ProcessInstanceKey,
			Key:                job.Key,
			State:              runtime.ActivityState(job.State),
			CreatedAt:          time.UnixMilli(job.CreatedAt),
			// BaseElement:        nil,
		}
	}
	return res, nil
}

var _ storage.JobStorageWriter = &RqLiteDB{}

func (rq *RqLiteDB) SaveJob(ctx context.Context, job runtime.Job) error {
	return SaveJobWith(ctx, rq.queries, job)
}

func SaveJobWith(ctx context.Context, db *sql.Queries, job runtime.Job) error {
	err := db.SaveJob(ctx, sql.SaveJobParams{
		Key:                job.GetKey(),
		ElementID:          job.ElementId,
		ElementInstanceKey: job.ElementInstanceKey,
		ProcessInstanceKey: job.ProcessInstanceKey,
		// Type:               job.Type, // TODO: add type to runtime.Job
		State:     int(job.GetState()),
		CreatedAt: job.CreatedAt.UnixMilli(),
		Variables: "{}", // TODO: add variables to job
	})
	if err != nil {
		return fmt.Errorf("failed to save job %d: %w", job.GetKey(), err)
	}
	return nil
}

var _ storage.MessageStorageReader = &RqLiteDB{}

func (rq *RqLiteDB) FindActivityMessageSubscriptions(ctx context.Context, originActivityKey int64, state runtime.ActivityState) ([]runtime.MessageSubscription, error) {
	dbMessages, err := rq.queries.FindActivityMessageSubscriptions(ctx, sql.FindActivityMessageSubscriptionsParams{
		OriginActivityKey: originActivityKey,
		State:             int(state),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find origin activity message subscriptions for activity %d: %w", originActivityKey, err)
	}
	res := make([]runtime.MessageSubscription, len(dbMessages))
	for i, mes := range dbMessages {
		res[i] = runtime.MessageSubscription{
			ElementId:            mes.ElementID,
			ElementInstanceKey:   mes.ElementInstanceKey,
			ProcessDefinitionKey: mes.ProcessDefinitionKey,
			ProcessInstanceKey:   mes.ProcessInstanceKey,
			Name:                 mes.Name,
			MessageState:         runtime.ActivityState(mes.State),
			CreatedAt:            time.UnixMilli(mes.CreatedAt),
			// OriginActivity:     mes.OriginActivityID,
			// BaseElement:        nil,
		}
	}
	return res, nil
}

func (rq *RqLiteDB) FindProcessInstanceMessageSubscriptions(ctx context.Context, processInstanceKey int64, state runtime.ActivityState) ([]runtime.MessageSubscription, error) {
	dbMessages, err := rq.queries.FindProcessInstanceMessageSubscriptions(ctx, sql.FindProcessInstanceMessageSubscriptionsParams{
		ProcessInstanceKey: processInstanceKey,
		State:              int(state),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find message subscriptions for process %d: %w", processInstanceKey, err)
	}
	res := make([]runtime.MessageSubscription, len(dbMessages))
	for i, mes := range dbMessages {
		res[i] = runtime.MessageSubscription{
			ElementId:            mes.ElementID,
			ElementInstanceKey:   mes.ElementInstanceKey,
			ProcessDefinitionKey: mes.ProcessDefinitionKey,
			ProcessInstanceKey:   mes.ProcessInstanceKey,
			Name:                 mes.Name,
			MessageState:         runtime.ActivityState(mes.State),
			CreatedAt:            time.UnixMilli(mes.CreatedAt),
			// OriginActivity:     mes.OriginActivityID,
			// BaseElement:        nil,
		}
	}
	return res, nil
}

var _ storage.MessageStorageWriter = &RqLiteDB{}

func (rq *RqLiteDB) SaveMessageSubscription(ctx context.Context, subscription runtime.MessageSubscription) error {
	return SaveMessageSubscriptionWith(ctx, rq.queries, subscription)
}
func SaveMessageSubscriptionWith(ctx context.Context, db *sql.Queries, subscription runtime.MessageSubscription) error {
	err := db.SaveMessageSubscription(ctx, sql.SaveMessageSubscriptionParams{
		Key:                  subscription.GetKey(),
		ElementInstanceKey:   subscription.ElementInstanceKey,
		ElementID:            subscription.ElementId,
		ProcessDefinitionKey: subscription.ProcessDefinitionKey,
		ProcessInstanceKey:   subscription.ProcessInstanceKey,
		Name:                 subscription.Name,
		State:                int(subscription.GetState()),
		CreatedAt:            subscription.CreatedAt.UnixMilli(),
		OriginActivityKey:    subscription.OriginActivity.GetKey(),
		OriginActivityState:  int(subscription.OriginActivity.GetState()),
		CorrelationKey:       "", // TODO: add message correlation keys into message subscription
		// OriginActivityID:     subscription.OriginActivity.Id(),
	})
	if err != nil {
		return fmt.Errorf("failed to save message subscription %d: %w", subscription.GetKey(), err)
	}
	return nil
}

type RqLiteDBBatch struct {
	db        *RqLiteDB
	stmtToRun []*proto.Statement
	queries   *sql.Queries
}

func (rq *RqLiteDBBatch) ExecContext(ctx context.Context, sql string, args ...interface{}) (ssql.Result, error) {
	stmt := rq.db.generateStatement(sql, args...)
	rq.stmtToRun = append(rq.stmtToRun, stmt)
	return rqliteResult{}, nil
}

func (rq *RqLiteDBBatch) PrepareContext(ctx context.Context, sql string) (*ssql.Stmt, error) {
	panic("PrepareContext not supported by RqLiteDBBatch")
}

func (rq *RqLiteDBBatch) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	panic("QueryContext not supported by RqLiteDBBatch")
}

func (rq *RqLiteDBBatch) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	panic("QueryRowContext not supported by RqLiteDBBatch")
}

var _ storage.Batch = &RqLiteDBBatch{}

func (b *RqLiteDBBatch) Flush(ctx context.Context) error {
	_, err := b.db.executeStatements(ctx, b.stmtToRun)
	return err
}

var _ storage.ProcessDefinitionStorageWriter = &RqLiteDBBatch{}

func (b *RqLiteDBBatch) SaveProcessDefinition(ctx context.Context, definition runtime.ProcessDefinition) error {
	return SaveProcessDefinitionWith(ctx, b.queries, definition)
}

var _ storage.ProcessInstanceStorageWriter = &RqLiteDBBatch{}

func (b *RqLiteDBBatch) SaveProcessInstance(ctx context.Context, processInstance runtime.ProcessInstance) error {
	return SaveProcessInstanceWith(ctx, b.queries, processInstance)
}

var _ storage.TimerStorageWriter = &RqLiteDBBatch{}

func (b *RqLiteDBBatch) SaveTimer(ctx context.Context, timer runtime.Timer) error {
	return SaveTimerWith(ctx, b.queries, timer)
}

var _ storage.JobStorageWriter = &RqLiteDBBatch{}

func (b *RqLiteDBBatch) SaveJob(ctx context.Context, job runtime.Job) error {
	return SaveJobWith(ctx, b.queries, job)
}

var _ storage.MessageStorageWriter = &RqLiteDBBatch{}

func (b *RqLiteDBBatch) SaveMessageSubscription(ctx context.Context, subscription runtime.MessageSubscription) error {
	return SaveMessageSubscriptionWith(ctx, b.queries, subscription)
}
