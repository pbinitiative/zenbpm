package rqlite

import (
	"context"
	sqlc "database/sql"
	"errors"

	"log"

	_ "embed"

	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/rqlite/rqlite/v8/command/proto"
)

type BpmnEnginePersistenceRqlite struct {
	//TODO: to remove
	// snowflakeIdGenerator *snowflake.Node
	store   storage.PersistentStorage
	queries *Queries
}

//go:embed sql/schema.sql
var ddl string

func NewBpmnEnginePersistenceRqlite( /*snowflakeIdGenerator *snowflake.Node, */ store storage.PersistentStorage) *BpmnEnginePersistenceRqlite {
	// gen := snowflakeIdGenerator
	rqlitePersistence := &BpmnEnginePersistenceRqlite{
		// snowflakeIdGenerator: gen,
		store: store,
	}

	queries := New(rqlitePersistence)

	rqlitePersistence.setQueries(queries)
	rqlitePersistence.ExecContext(context.Background(), ddl)

	return rqlitePersistence
}

// READ
func (persistence *BpmnEnginePersistenceRqlite) FindProcesses(ctx context.Context, bpmnProcessId string, processDefinitionKey int64) ([]ProcessDefinition, error) {

	params := FindProcessDefinitionsParams{
		Key:           sqlc.NullInt64{Int64: processDefinitionKey, Valid: processDefinitionKey != -1},
		BpmnProcessID: sqlc.NullString{String: bpmnProcessId, Valid: bpmnProcessId != ""},
	}

	// Fetch process instances
	definitions, err := persistence.queries.FindProcessDefinitions(ctx, params)
	if err != nil {
		log.Fatal("Finding process instance failed", err)
		return nil, err
	}

	return definitions, nil
}

func (persistence *BpmnEnginePersistenceRqlite) FindProcessInstances(ctx context.Context, processInstanceKey int64, processDefinitionKey int64) ([]ProcessInstance, error) {

	params := FindProcessInstancesParams{
		Key:                  sqlc.NullInt64{Int64: processInstanceKey, Valid: processInstanceKey != -1},
		ProcessDefinitionKey: sqlc.NullInt64{Int64: processDefinitionKey, Valid: processDefinitionKey != -1},
	}

	// Fetch process instances
	instances, err := persistence.queries.FindProcessInstances(ctx, params)
	if err != nil {
		log.Fatal("Finding process instance failed", err)
		return nil, err
	}

	return instances, nil
}

func (persistence *BpmnEnginePersistenceRqlite) FindMessageSubscription(ctx context.Context, originActivityKey int64, processInstanceKey int64, elementId string, state []string) ([]MessageSubscription, error) {

	params := FindMessageSubscriptionsParams{
		OriginActivityKey:  sqlc.NullInt64{Int64: originActivityKey, Valid: originActivityKey != -1},
		ProcessInstanceKey: sqlc.NullInt64{Int64: processInstanceKey, Valid: processInstanceKey != -1},
		ElementID:          sqlc.NullString{String: elementId, Valid: elementId != ""},
	}

	// Fetch subscriptions
	messageSubscriptions, err := persistence.queries.FindMessageSubscriptions(ctx, params)
	if err != nil {
		log.Fatal("Finding message subscriptions failed", err)
		return nil, err
	}
	resultSubscriptions := make([]MessageSubscription, len(messageSubscriptions))
	for i, ms := range messageSubscriptions {
		resultSubscriptions[i] = MessageSubscription(ms)
	}
	return resultSubscriptions, nil
}

func (persistence *BpmnEnginePersistenceRqlite) FindTimers(ctx context.Context, elementInstanceKey int64, processInstanceKey int64, state []string) ([]Timer, error) {
	params := FindTimersParams{
		ProcessInstanceKey: sqlc.NullInt64{Int64: processInstanceKey, Valid: processInstanceKey != -1},
		ElementInstanceKey: sqlc.NullInt64{Int64: elementInstanceKey, Valid: elementInstanceKey != -1},
	}

	// Fetch timers
	timers, err := persistence.queries.FindTimers(ctx, params)
	if err != nil {
		log.Fatal("Finding timers failed", err)
		return nil, err
	}
	resultTimers := make([]Timer, len(timers))
	for i, t := range timers {
		resultTimers[i] = Timer(t)
	}
	return resultTimers, nil
}

func (persistence *BpmnEnginePersistenceRqlite) FindJobs(ctx context.Context, elementId string, processInstanceKey int64, jobKey int64, state []string) ([]Job, error) {

	params := FindJobsParams{
		Key:                sqlc.NullInt64{Int64: jobKey, Valid: jobKey != -1},
		ProcessInstanceKey: sqlc.NullInt64{Int64: processInstanceKey, Valid: processInstanceKey != -1},
		ElementID:          sqlc.NullString{String: elementId, Valid: elementId != ""},
	}

	// Fetch jobs
	jobs, err := persistence.queries.FindJobs(ctx, params)
	if err != nil {
		log.Fatal("Finding jobs failed", err)
		return nil, err
	}
	return jobs, nil
}

func (persistence *BpmnEnginePersistenceRqlite) FindActivitiesByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]ActivityInstance, error) {
	// Fetch activities
	activities, err := persistence.queries.FindActivityInstances(ctx, sqlc.NullInt64{Int64: processInstanceKey, Valid: processInstanceKey != -1})
	if err != nil {
		log.Fatal("Finding activities failed", err)
		return nil, err
	}
	resultActivities := make([]ActivityInstance, len(activities))
	for i, a := range activities {
		resultActivities[i] = ActivityInstance(a)
	}
	return resultActivities, nil
}

// WRITE

func (persistence *BpmnEnginePersistenceRqlite) SaveNewProcess(ctx context.Context, processDefinition *ProcessDefinition) error {
	return persistence.queries.SaveProcessDefinition(ctx, SaveProcessDefinitionParams(*processDefinition))

}

func (persistence *BpmnEnginePersistenceRqlite) SaveProcessInstance(ctx context.Context, processInstance *ProcessInstance) error {
	return persistence.queries.SaveProcessInstance(ctx, SaveProcessInstanceParams(*processInstance))
}

func (persistence *BpmnEnginePersistenceRqlite) SaveMessageSubscription(ctx context.Context, subscription *MessageSubscription) error {
	return persistence.queries.SaveMessageSubscription(ctx, SaveMessageSubscriptionParams(*subscription))
}

func (persistence *BpmnEnginePersistenceRqlite) SaveTimer(ctx context.Context, timer *Timer) error {
	return persistence.queries.SaveTimer(ctx, SaveTimerParams(*timer))
}

func (persistence *BpmnEnginePersistenceRqlite) SaveJob(ctx context.Context, job *Job) error {
	return persistence.queries.SaveJob(ctx, SaveJobParams(*job))
}

func (persistence *BpmnEnginePersistenceRqlite) SaveActivity(ctx context.Context, activity ActivityInstance) error {
	return persistence.queries.SaveActivityInstance(ctx, SaveActivityInstanceParams(activity))

}

func (persistence *BpmnEnginePersistenceRqlite) IsLeader() bool {
	return persistence.store.IsLeader(context.Background())
}

func (persistence *BpmnEnginePersistenceRqlite) setQueries(queries *Queries) {
	persistence.queries = queries
}

func execute(statement string, store storage.PersistentStorage, parameters ...interface{}) ([]*proto.ExecuteQueryResponse, error) {
	stmt := generateStatment(statement, parameters...)

	er := &proto.ExecuteRequest{
		Request: &proto.Request{
			Transaction: true,
			DbTimeout:   int64(0),
			Statements:  []*proto.Statement{stmt},
		},
		Timings: false,
	}

	results, resultsErr := store.Execute(context.Background(), er)

	if resultsErr != nil {
		log.Panicf("Error executing SQL statements %s", resultsErr)
		return nil, resultsErr
	}
	log.Printf("Result: %v", results)
	return results, nil
}

func generateStatment(sql string, parameters ...interface{}) *proto.Statement {
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
			log.Panicf("Unknown parameter type: %T", par)
		}

	}
	return &proto.Statement{
		Sql:        sql,
		Parameters: resultParams,
	}
}

func query(query string, store storage.PersistentStorage, parameters ...interface{}) ([]*proto.QueryRows, error) {

	stmts := generateStatment(query, parameters...)

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
		log.Fatalf("Error executing SQL statements %s", resultsErr)
		return nil, resultsErr
	}
	log.Printf("Result: %v", results)
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
	result, err := execute(sql, persistence.store, args...)

	if err != nil {
		log.Panicf("Error executing SQL statements")
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

func (persistence *BpmnEnginePersistenceRqlite) PrepareContext(ctx context.Context, sql string) (*sqlc.Stmt, error) {
	return nil, errors.New("PrepareContext not supported by rqlite")
}

func (persistence *BpmnEnginePersistenceRqlite) QueryContext(ctx context.Context, sql string, args ...interface{}) (*Rows, error) {
	results, err := query(sql, persistence.store, args...)
	if err != nil {
		return nil, err
	}
	if len(results) > 1 {
		return nil, errors.New("Multiple results not supported")
	}
	for _, r := range results {
		return constructRows(ctx, r.Columns, r.Types, r.Values), nil
	}
	// empty results
	return constructRows(ctx, []string{}, []string{}, []*proto.Values{}), nil
}

func (persistence *BpmnEnginePersistenceRqlite) QueryRowContext(ctx context.Context, sql string, args ...interface{}) *Row {
	rows, err := persistence.QueryContext(ctx, sql, args...)
	if err != nil {
		return &Row{ctx: ctx, err: err}
	}
	defer rows.Close()

	row := rows.Next()
	if !row {
		return &Row{} // Returning empty sql.Row on error
	} else {
		row := &Row{
			columns: rows.columns,
			types:   rows.types,
			values:  rows.values[0],
			ctx:     ctx,
			err:     nil,
		}
		return row
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
