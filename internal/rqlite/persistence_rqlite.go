package rqlite

import (
	"context"
	sqlc "database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	_ "embed"

	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/rqlite/sql"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/rqlite/rqlite/v8/command/proto"
)

type BpmnEnginePersistenceRqlite struct {
	//TODO: to remove
	// snowflakeIdGenerator *snowflake.Node
	store   storage.PersistentStorage
	queries *sql.Queries
}

//go:embed sql_source/schema.sql
var ddl string

func NewBpmnEnginePersistenceRqlite( /*snowflakeIdGenerator *snowflake.Node, */ store storage.PersistentStorage) *BpmnEnginePersistenceRqlite {
	// gen := snowflakeIdGenerator
	rqlitePersistence := &BpmnEnginePersistenceRqlite{
		// snowflakeIdGenerator: gen,
		store: store,
	}

	queries := sql.New(rqlitePersistence)

	rqlitePersistence.setQueries(queries)
	rqlitePersistence.ExecContext(context.Background(), ddl)

	return rqlitePersistence
}

// READ
func (persistence *BpmnEnginePersistenceRqlite) FindProcesses(ctx context.Context, bpmnProcessId *string, processDefinitionKey *int64) ([]sql.ProcessDefinition, error) {

	params := sql.FindProcessDefinitionsParams{
		Key:           sqlc.NullInt64{Int64: ptr.Deref(processDefinitionKey, int64(0)), Valid: processDefinitionKey != nil},
		BpmnProcessID: sqlc.NullString{String: ptr.Deref(bpmnProcessId, ""), Valid: bpmnProcessId != nil},
	}

	// Fetch process instances
	definitions, err := persistence.queries.FindProcessDefinitions(ctx, params)
	if err != nil {
		log.Error("Finding process instance failed with error %v", err)
		return nil, err
	}

	return definitions, nil
}

func (persistence *BpmnEnginePersistenceRqlite) FindProcessInstances(ctx context.Context, processInstanceKey *int64, processDefinitionKey *int64) ([]sql.ProcessInstance, error) {

	params := sql.FindProcessInstancesParams{
		Key:                  sqlc.NullInt64{Int64: ptr.Deref(processInstanceKey, int64(0)), Valid: processInstanceKey != nil},
		ProcessDefinitionKey: sqlc.NullInt64{Int64: ptr.Deref(processDefinitionKey, int64(0)), Valid: processDefinitionKey != nil},
	}

	// Fetch process instances
	instances, err := persistence.queries.FindProcessInstances(ctx, params)
	if err != nil {
		log.Error("Finding process instance failed with error %v", err)
		return nil, err
	}

	return instances, nil
}

func (persistence *BpmnEnginePersistenceRqlite) FindMessageSubscriptions(ctx context.Context, originActivityKey *int64, processInstanceKey *int64, elementId *string, state []string) ([]sql.MessageSubscription, error) {

	params := sql.FindMessageSubscriptionsParams{
		OriginActivityKey:  sqlc.NullInt64{Int64: ptr.Deref(originActivityKey, int64(0)), Valid: originActivityKey != nil},
		ProcessInstanceKey: sqlc.NullInt64{Int64: ptr.Deref(processInstanceKey, int64(0)), Valid: processInstanceKey != nil},
		ElementID:          sqlc.NullString{String: ptr.Deref(elementId, ""), Valid: elementId != nil},
		States:             sqlc.NullString{String: serializeState(state, activityStateMap), Valid: state != nil},
	}

	// Fetch subscriptions
	messageSubscriptions, err := persistence.queries.FindMessageSubscriptions(ctx, params)
	if err != nil {
		log.Error("Finding message subscriptions failed with error %v", err)
		return nil, err
	}
	resultSubscriptions := make([]sql.MessageSubscription, len(messageSubscriptions))
	for i, ms := range messageSubscriptions {
		resultSubscriptions[i] = sql.MessageSubscription(ms)
	}
	return resultSubscriptions, nil
}

func (persistence *BpmnEnginePersistenceRqlite) FindTimers(ctx context.Context, elementInstanceKey *int64, processInstanceKey *int64, state []string) ([]sql.Timer, error) {
	params := sql.FindTimersParams{
		ProcessInstanceKey: sqlc.NullInt64{Int64: ptr.Deref(processInstanceKey, int64(0)), Valid: processInstanceKey != nil},
		ElementInstanceKey: sqlc.NullInt64{Int64: ptr.Deref(elementInstanceKey, int64(0)), Valid: elementInstanceKey != nil},
		States:             sqlc.NullString{String: serializeState(state, timerStateMap), Valid: state != nil},
	}

	// Fetch timers
	timers, err := persistence.queries.FindTimers(ctx, params)
	if err != nil {
		log.Error("Finding timers failed with error %v", err)
		return nil, err
	}
	resultTimers := make([]sql.Timer, len(timers))
	for i, t := range timers {
		resultTimers[i] = sql.Timer(t)
	}
	return resultTimers, nil
}

func interfaceSlice(slice []string) []interface{} {
	ret := make([]interface{}, len(slice))
	for i, v := range slice {
		ret[i] = v
	}
	return ret
}

func (persistence *BpmnEnginePersistenceRqlite) FindJobs(ctx context.Context, elementId *string, processInstanceKey *int64, jobKey *int64, state []string) ([]sql.Job, error) {
	params := sql.FindJobsWithStatesParams{
		Key:                sqlc.NullInt64{Int64: ptr.Deref(jobKey, int64(-1)), Valid: jobKey != nil},
		ProcessInstanceKey: sqlc.NullInt64{Int64: ptr.Deref(processInstanceKey, int64(-1)), Valid: processInstanceKey != nil},
		ElementID:          sqlc.NullString{String: ptr.Deref(elementId, ""), Valid: elementId != nil},
		States:             sqlc.NullString{String: serializeState(state, activityStateMap), Valid: state != nil},
	}
	// Fetch jobs
	jobs, err := persistence.queries.FindJobsWithStates(ctx, params)
	if err != nil {
		log.Error("Finding jobs failed with error %v", err)
		return nil, err
	}
	return jobs, nil
}

func (persistence *BpmnEnginePersistenceRqlite) FindActivitiesByProcessInstanceKey(ctx context.Context, processInstanceKey *int64) ([]sql.ActivityInstance, error) {
	// Fetch activities
	activities, err := persistence.queries.FindActivityInstances(ctx, sqlc.NullInt64{Int64: ptr.Deref(processInstanceKey, int64(-1)), Valid: processInstanceKey != nil})
	if err != nil {
		log.Error("Finding activities failed with error %v", err)
		return nil, err
	}
	resultActivities := make([]sql.ActivityInstance, len(activities))
	for i, a := range activities {
		resultActivities[i] = sql.ActivityInstance(a)
	}
	return resultActivities, nil
}

// WRITE

func (persistence *BpmnEnginePersistenceRqlite) SaveNewProcess(ctx context.Context, processDefinition sql.ProcessDefinition) error {
	return persistence.queries.SaveProcessDefinition(ctx, sql.SaveProcessDefinitionParams(processDefinition))

}

func (persistence *BpmnEnginePersistenceRqlite) SaveProcessInstance(ctx context.Context, processInstance sql.ProcessInstance) error {
	return persistence.queries.SaveProcessInstance(ctx, sql.SaveProcessInstanceParams(processInstance))
}

func (persistence *BpmnEnginePersistenceRqlite) SaveMessageSubscription(ctx context.Context, subscription sql.MessageSubscription) error {
	return persistence.queries.SaveMessageSubscription(ctx, sql.SaveMessageSubscriptionParams(subscription))
}

func (persistence *BpmnEnginePersistenceRqlite) SaveTimer(ctx context.Context, timer sql.Timer) error {
	return persistence.queries.SaveTimer(ctx, sql.SaveTimerParams(timer))
}

func (persistence *BpmnEnginePersistenceRqlite) SaveJob(ctx context.Context, job sql.Job) error {
	return persistence.queries.SaveJob(ctx, sql.SaveJobParams(job))
}

func (persistence *BpmnEnginePersistenceRqlite) SaveActivity(ctx context.Context, activity sql.ActivityInstance) error {
	return persistence.queries.SaveActivityInstance(ctx, sql.SaveActivityInstanceParams(activity))

}

func (persistence *BpmnEnginePersistenceRqlite) IsLeader() bool {
	return persistence.store.IsLeader(context.Background())
}

func (persistence *BpmnEnginePersistenceRqlite) setQueries(queries *sql.Queries) {
	persistence.queries = queries
}

func executeStatements(ctx context.Context, statements []*proto.Statement, store storage.PersistentStorage) ([]*proto.ExecuteQueryResponse, error) {
	er := &proto.ExecuteRequest{
		Request: &proto.Request{
			Transaction: true,
			DbTimeout:   int64(0),
			Statements:  statements,
		},
		Timings: false,
	}

	results, resultsErr := store.Execute(ctx, er)

	if resultsErr != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Errorf(ctx, "Deadline exceeded form statement executionId %d", ctx.Value("executionId"))
		}
		log.Error("Error executing SQL statements %s", resultsErr)
		return nil, resultsErr
	}
	log.Info("Result: %v", results)
	return results, nil
}

func generateStatement(sql string, parameters ...interface{}) *proto.Statement {
	resultParams := make([]*proto.Parameter, 0)

	for _, par := range parameters {
		switch par.(type) {
		case string:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_S{
					S: par.(string),
				},
			})
		case int64:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_I{
					I: par.(int64),
				},
			})
		case int32:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_I{
					I: int64(par.(int32)),
				},
			})
		case int:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_I{
					I: int64(par.(int)),
				},
			})
		case float64:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_D{
					D: par.(float64),
				},
			})
		case bool:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_B{
					B: par.(bool),
				},
			})
		case []byte:
			resultParams = append(resultParams, &proto.Parameter{
				Value: &proto.Parameter_Y{
					Y: par.([]byte),
				},
			})
		case sqlc.NullInt64:
			if par.(sqlc.NullInt64).Valid {
				resultParams = append(resultParams, &proto.Parameter{
					Value: &proto.Parameter_I{
						I: par.(sqlc.NullInt64).Int64,
					},
				})
			} else {
				resultParams = append(resultParams, &proto.Parameter{})
			}
		case sqlc.NullString:
			if par.(sqlc.NullString).Valid {
				resultParams = append(resultParams, &proto.Parameter{
					Value: &proto.Parameter_S{
						S: par.(sqlc.NullString).String,
					},
				})
			} else {
				resultParams = append(resultParams, &proto.Parameter{})
			}
		default:
			log.Error("Unknown parameter type: %T", par)
		}

	}
	return &proto.Statement{
		Sql:        sql,
		Parameters: resultParams,
	}
}

func queryDatabase(query string, store storage.PersistentStorage, parameters ...interface{}) ([]*proto.QueryRows, error) {

	stmts := generateStatement(query, parameters...)

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

	results, resultsErr := store.Query(context.Background(), qr)
	if resultsErr != nil {
		log.Error("Error executing SQL statements %s", resultsErr)
		return nil, resultsErr
	}
	log.Info("Result: %v", results)
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

func (persistence *BpmnEnginePersistenceRqlite) ExecContext(ctx context.Context, sql string, args ...interface{}) (sqlc.Result, error) {
	if ctx.Value("executionKey") == nil {
		// when `ExecContext` called outside execution context identified by `executionId` do the normal execution
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		result, err := executeStatements(ctxWithTimeout, []*proto.Statement{generateStatement(sql, args...)}, persistence.store)

		if err != nil {
			log.Error("Error executing SQL statements")
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

	} else {
		// when `ExecContext` called inside execution context identified by `executionId` add the statement to the transaction
		persistence.addToTransaction(ctx.Value("executionKey").(int64), generateStatement(sql, args...))
		return rqliteResult{}, nil
	}

}

func (persistence *BpmnEnginePersistenceRqlite) PrepareContext(ctx context.Context, sql string) (*sqlc.Stmt, error) {
	return nil, errors.New("PrepareContext not supported by rqlite")
}

func (persistence *BpmnEnginePersistenceRqlite) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	results, err := queryDatabase(query, persistence.store, args...)
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

func (persistence *BpmnEnginePersistenceRqlite) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	rows, err := persistence.QueryContext(ctx, query, args...)
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

var activityStateMap = map[string]int{
	"ACTIVE":       1,
	"COMPENSATED":  2,
	"COMPENSATING": 3,
	"COMPLETED":    4,
	"COMPLETING":   5,
	"FAILED":       6,
	"FAILING":      7,
	"READY":        8,
	"TERMINATED":   9,
	"TERMINATING":  10,
	"WITHDRAWN":    11,
}

func convertStateArray(state []string, stateMap map[string]int) []string {
	var result []string
	for _, s := range state {
		mapped := stateMap[s]
		if mapped == 0 {
			continue
		}
		result = append(result, strconv.Itoa(mapped))
	}
	return result
}

func serializeState(state []string, stateMap map[string]int) string {
	return "[" + strings.Join(convertStateArray(state, stateMap), ",") + "]"
}

// reverse the map
func reverseMap[K comparable, V comparable](m map[K]V) map[V]K {
	rm := make(map[V]K)
	for k, v := range m {
		rm[v] = k
	}
	return rm
}

var timerStateMap = map[string]int{
	"TIMERCREATED":   1,
	"TIMERTIGGERED":  2,
	"TIMERCANCELLED": 3,
}

func convertTimerStateArray(state []string) []int {
	var result []int
	for _, s := range state {
		result = append(result, timerStateMap[s])
	}
	return result

}
