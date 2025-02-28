package rqlite

import (
	"context"
	sqlc "database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"log"

	_ "embed"

	bpmnEngineExporter "github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	sql "github.com/pbinitiative/zenbpm/pkg/bpmn/persistence/rqlite/sql"
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
func (persistence *BpmnEnginePersistenceRqlite) FindProcesses(processId string, processKey int64) []*sql.ProcessDefinitionEntity {
	// TODO finds all processes with given ID sorted by version number

	filters := map[string]string{}

	if processId != "" {
		filters["bpmn_process_id"] = fmt.Sprintf("\"%s\"", processId)
	}
	if processKey != -1 {
		filters["key"] = fmt.Sprintf("%d", processKey)
	}

	whereClause := generateWhereClause(filters)

	queryStr := fmt.Sprintf(sql.PROCESS_DEFINITION, whereClause)

	rows, err := query(queryStr, persistence.store)
	if err != nil {
		log.Fatalf("Error executing SQL statements %s", err)
		return nil
	}
	log.Printf("Result: %T", rows)

	var processDefinitions []*sql.ProcessDefinitionEntity = make([]*sql.ProcessDefinitionEntity, 0)

	for _, qr := range rows {

		for _, r := range qr.Values {
			def := new(sql.ProcessDefinitionEntity)

			def.Key = (*r.Parameters[0]).GetI()
			def.Version = int32((*r.Parameters[1]).GetI())
			def.BpmnProcessId = (*r.Parameters[2]).GetS()
			dataString := (*r.Parameters[3]).GetS()

			encodedXml, err := base64.StdEncoding.DecodeString(dataString)
			if err != nil {
				log.Fatalf("Error base64 decoding bpmn data: %s", err)
				return nil
			}

			def.BpmnData = string(encodedXml)

			bytes, err := base64.StdEncoding.DecodeString((*r.Parameters[4]).GetS())

			if err != nil {
				log.Fatalf("Error decoding checksum: %s", err)
				return nil
			}

			def.BpmnChecksum = bytes
			// copy(def.BpmnChecksum[:], bytes)

			def.BpmnResourceName = (*r.Parameters[5]).GetS()

			processDefinitions = append(processDefinitions, def)

		}

	}
	return processDefinitions
}

func (persistence *BpmnEnginePersistenceRqlite) FindProcessInstances(processInstanceKey int64, processDefinitionKey int64) []ProcessInstance {

	params := GetProcessInstancesParams{
		Key:                  sqlc.NullInt64{Int64: processInstanceKey, Valid: processInstanceKey != -1},
		ProcessDefinitionKey: sqlc.NullInt64{Int64: processDefinitionKey, Valid: processDefinitionKey != -1},
	}

	// Fetch process instances
	instances, err := persistence.queries.GetProcessInstances(context.Background(), params)
	if err != nil {
		log.Fatal("Finding process instance failed", err)
	}

	return instances
}

func (persistence *BpmnEnginePersistenceRqlite) FindMessageSubscription(originActivityKey int64, processInstanceKey int64, elementId string, state []string) []*sql.MessageSubscriptionEntity {
	filters := map[string]string{}

	if originActivityKey != -1 {
		filters["origin_activity_key"] = fmt.Sprintf("%d", originActivityKey)
	}
	if processInstanceKey != -1 {
		filters["process_instance_key"] = fmt.Sprintf("%d", processInstanceKey)
	}
	if elementId != "" {
		filters["element_id"] = fmt.Sprintf("\"%s\"", elementId)
	}

	statesClause := ""
	if len(state) > 0 {
		states := map[string]string{}
		// for each state
		for _, s := range state {
			states["state"] = fmt.Sprintf("%d", activityStateMap[s])
		}
		statesClause = fmt.Sprintf(" AND (%s)", whereClauseBuilder(states, "OR"))
	}

	whereClause := generateWhereClause(filters) + statesClause

	queryStr := fmt.Sprintf(sql.MESSAGE_SUBSCRIPTION_SELECT, whereClause)

	rows, err := query(queryStr, persistence.store)
	if err != nil {
		log.Fatalf("Error executing SQL statements %s", err)
		return nil
	}
	log.Printf("Result: %T", rows)

	var messageSubscriptions []*sql.MessageSubscriptionEntity = make([]*sql.MessageSubscriptionEntity, 0)

	for _, qr := range rows {

		for _, r := range qr.Values {
			def := new(sql.MessageSubscriptionEntity)
			def.ElementInstanceKey = (*r.Parameters[0]).GetI()
			def.ElementID = (*r.Parameters[1]).GetS()
			def.ProcessKey = (*r.Parameters[2]).GetI()
			def.ProcessInstanceKey = (*r.Parameters[3]).GetI()
			def.MessageName = (*r.Parameters[4]).GetS()
			def.State = int((*r.Parameters[5]).GetI())
			def.CreatedAt = (*r.Parameters[6]).GetI()
			def.OriginActivityKey = int64((*r.Parameters[7]).GetI())
			def.OriginActivityState = int((*r.Parameters[8]).GetI())
			def.OriginActivityId = (*r.Parameters[9]).GetS()

			messageSubscriptions = append(messageSubscriptions, def)

		}

	}
	return messageSubscriptions
}

func (persistence *BpmnEnginePersistenceRqlite) FindTimers(originActivityKey int64, processInstanceKey int64, state []string) []*sql.TimerEntity {
	filters := map[string]string{}

	if originActivityKey != -1 {
		filters["origin_activity_key"] = fmt.Sprintf("%d", originActivityKey)
	}
	if processInstanceKey != -1 {
		filters["process_instance_key"] = fmt.Sprintf("%d", processInstanceKey)
	}

	statesClause := ""
	if len(state) > 0 {
		states := map[string]string{}
		// for each state
		for _, s := range state {
			states["state"] = fmt.Sprintf("%d", timerStateMap[s])
		}
		statesClause = fmt.Sprintf(" AND (%s)", whereClauseBuilder(states, "OR"))
	}

	whereClause := generateWhereClause(filters) + statesClause

	queryStr := fmt.Sprintf(sql.TIMER_SELECT, whereClause)

	rows, err := query(queryStr, persistence.store)
	if err != nil {
		log.Fatalf("Error executing SQL statements %s", err)
		return nil
	}
	log.Printf("Result: %T", rows)

	var timers []*sql.TimerEntity = make([]*sql.TimerEntity, 0)

	for _, qr := range rows {

		for _, r := range qr.Values {
			def := new(sql.TimerEntity)
			def.ElementID = (*r.Parameters[0]).GetS()
			def.ElementInstanceKey = (*r.Parameters[1]).GetI()
			def.ProcessKey = (*r.Parameters[2]).GetI()
			def.ProcessInstanceKey = (*r.Parameters[3]).GetI()
			def.TimerState = (*r.Parameters[4]).GetI()
			def.CreatedAt = (*r.Parameters[5]).GetI()
			def.DueDate = (*r.Parameters[6]).GetI()
			def.Duration = (*r.Parameters[7]).GetI()

			timers = append(timers, def)

		}

	}
	return timers
}

func (persistence *BpmnEnginePersistenceRqlite) FindJobs(elementId string, processInstanceKey int64, jobKey int64, state []string) []*sql.JobEntity {
	filters := map[string]string{}

	if elementId != "" {
		filters["element_id"] = fmt.Sprintf("\"%s\"", elementId)
	}
	if processInstanceKey != -1 {
		filters["process_instance_key"] = fmt.Sprintf("%d", processInstanceKey)
	}
	if jobKey != -1 {
		filters["key"] = fmt.Sprintf("%d", jobKey)
	}

	statesClause := ""
	if len(state) > 0 {
		states := map[string]string{}
		// for each state
		for _, s := range state {
			states["state"] = fmt.Sprintf("%d", activityStateMap[s])
		}
		statesClause = fmt.Sprintf(" AND (%s)", whereClauseBuilder(states, "OR"))
	}

	whereClause := generateWhereClause(filters) + statesClause

	queryStr := fmt.Sprintf(sql.JOB_SELECT, whereClause)

	rows, err := query(queryStr, persistence.store)
	if err != nil {
		log.Fatalf("Error executing SQL statements %s", err)
		return nil
	}
	log.Printf("Result: %T", rows)

	var jobs []*sql.JobEntity = make([]*sql.JobEntity, 0)

	for _, qr := range rows {

		for _, r := range qr.Values {
			def := new(sql.JobEntity)

			def.Key = (*r.Parameters[0]).GetI()
			def.ElementID = (*r.Parameters[1]).GetS()
			def.ElementInstanceKey = int64((*r.Parameters[2]).GetI())
			def.ProcessInstanceKey = int64((*r.Parameters[3]).GetI())
			def.State = (*r.Parameters[4]).GetI()
			def.CreatedAt = (*r.Parameters[5]).GetI()

			jobs = append(jobs, def)

		}

	}
	return jobs
}

func (persistence *BpmnEnginePersistenceRqlite) FindActivitiesByProcessInstanceKey(processInstanceKey int64) []*sql.ActivityInstanceEntity {
	filters := map[string]string{}

	if processInstanceKey != -1 {
		filters["process_instance_key"] = fmt.Sprintf("%d", processInstanceKey)
	}

	whereClause := generateWhereClause(filters)

	queryStr := fmt.Sprintf(sql.ACTIVITY_INSTANCE_SELECT, whereClause)

	rows, err := query(queryStr, persistence.store)
	if err != nil {
		log.Fatalf("Error executing SQL statements %s", err)
		return nil
	}
	log.Printf("Result: %T", rows)

	var activites []*sql.ActivityInstanceEntity = make([]*sql.ActivityInstanceEntity, 0)

	for _, qr := range rows {

		for _, r := range qr.Values {
			def := new(sql.ActivityInstanceEntity)
			// key, process_instance_key, process_definition_key, created_at, state, element_id, bpmn_element_type

			def.Key = (*r.Parameters[0]).GetI()
			def.ProcessInstanceKey = int64((*r.Parameters[1]).GetI())
			def.ProcessDefinitionKey = int64((*r.Parameters[2]).GetI())
			def.CreatedAt = (*r.Parameters[3]).GetI()
			def.State = (*r.Parameters[4]).GetS()
			def.ElementId = (*r.Parameters[5]).GetS()
			def.BpmnElementType = (*r.Parameters[6]).GetS()

			activites = append(activites, def)

		}

	}
	return activites
}

// WRITE

func (persistence *BpmnEnginePersistenceRqlite) PersistNewProcess(processDefinition *sql.ProcessDefinitionEntity) error {

	sql := sql.BuildProcessDefinitionUpsertQuery(processDefinition)

	log.Printf("Creating process definition: %s", sql)

	_, err := execute(sql, persistence.store)
	if err != nil {
		log.Fatalf("Error executing SQL statements: %s", err)
		return err
	}
	return nil

}

func (persistence *BpmnEnginePersistenceRqlite) PersistProcessInstance(processInstance *sql.ProcessInstanceEntity) error {

	sql := sql.BuildProcessInstanceUpsertQuery(processInstance)

	log.Printf("Creating process instance: %s", sql)
	_, err := execute(sql, persistence.store)

	if err != nil {
		log.Panicf("Error executing SQL statements")
		return err
	}
	return nil

}

func (persistence *BpmnEnginePersistenceRqlite) PersistProcessInstanceNew(ctx context.Context, processInstance *sql.ProcessInstanceEntity) error {
	err := persistence.queries.InsertProcessInstance(ctx, InsertProcessInstanceParams(*processInstance))
	if err != nil {
		return err
	}
	return nil
}

func (persistence *BpmnEnginePersistenceRqlite) PersistNewMessageSubscription(subscription *sql.MessageSubscriptionEntity) error {
	sql := sql.BuildMessageSubscriptionUpsertQuery(subscription)

	log.Printf("Creating message subscription: %s", sql)
	_, err := execute(sql, persistence.store)

	if err != nil {
		log.Panicf("Error executing SQL statements")
		return err
	}
	return nil
}

func (persistence *BpmnEnginePersistenceRqlite) PersistNewTimer(timer *sql.TimerEntity) error {
	sql := sql.BuildTimerUpsertQuery(timer)

	log.Printf("Creating timer: %s", sql)
	_, err := execute(sql, persistence.store)

	if err != nil {
		log.Panicf("Error executing SQL statements")
		return err
	}
	return nil
}

func (persistence *BpmnEnginePersistenceRqlite) PersistJob(job *sql.JobEntity) error {
	sql := sql.BuildJobUpsertQuery(job)

	log.Printf("Creating job: %s", sql)
	_, err := execute(sql, persistence.store)

	if err != nil {
		log.Panicf("Error executing SQL statements")
		return err
	}
	return nil
}

func (persistence *BpmnEnginePersistenceRqlite) PersistActivity(event *bpmnEngineExporter.ProcessInstanceEvent, elementInfo *bpmnEngineExporter.ElementInfo) error {
	// sql := sql.BuildActivityInstanceUpsertQuery(persistence.snowflakeIdGenerator.Generate().Int64(), event.ProcessInstanceKey, event.ProcessKey, time.Now().Unix(), elementInfo.Intent, elementInfo.ElementId, elementInfo.BpmnElementType)
	sql := sql.BuildActivityInstanceUpsertQuery(-1, event.ProcessInstanceKey, event.ProcessKey, time.Now().Unix(), elementInfo.Intent, elementInfo.ElementId, elementInfo.BpmnElementType)

	log.Printf("Creating activity log: %s", sql)
	_, err := execute(sql, persistence.store)

	if err != nil {
		log.Panicf("Error executing SQL statements")
		return err
	}
	return nil
}

func (persistence *BpmnEnginePersistenceRqlite) IsLeader() bool {
	return persistence.store.IsLeader(context.Background())
}

func (persistence *BpmnEnginePersistenceRqlite) setQueries(queries *Queries) {
	persistence.queries = queries
}

func generateWhereClause(mappings map[string]string) string {
	where := whereClauseBuilder(mappings, "AND")

	if where == "" {
		return "1=1"
	}
	return where
}

func whereClauseBuilder(mappings map[string]string, operator string) string {
	wheres := make([]string, 0)
	for k, v := range mappings {
		wheres = append(wheres, fmt.Sprintf("%s = %s", k, v))
	}
	return strings.Join(wheres, " "+operator+" ")

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
				resultParams = append(resultParams, &proto.Parameter{
					Value: &proto.Parameter_I{},
				})
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
	// rows, err := persistence.QueryContext(ctx, sql, args...)
	// if err != nil {
	// 	return &sqlc.Row{} // Returning empty sql.Row on error
	// }
	// defer rows.Close()

	// return rowsToRow(rows)
	return nil
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
