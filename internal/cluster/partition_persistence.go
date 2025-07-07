package cluster

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"strings"
	"time"

	ssql "database/sql"

	"github.com/bwmarrin/snowflake"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/pbinitiative/zenbpm/internal/config"
	otelPkg "github.com/pbinitiative/zenbpm/internal/otel"
	"github.com/pbinitiative/zenbpm/internal/profile"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/store"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type RqLiteDB struct {
	store     *store.Store
	queries   *sql.Queries
	logger    hclog.Logger
	node      *snowflake.Node
	partition uint32
	tracer    trace.Tracer
	pdCache   *expirable.LRU[int64, runtime.ProcessDefinition]
}

// GenerateId implements storage.Storage.
func (rq *RqLiteDB) GenerateId() int64 {
	return rq.node.Generate().Int64()
}

func NewRqLiteDB(store *store.Store, partition uint32, logger hclog.Logger, cfg config.Persistence) (*RqLiteDB, error) {
	node, err := snowflake.NewNode(int64(partition))
	if err != nil {
		return nil, fmt.Errorf("failed to create snowflake node for partition %d: %w", partition, err)
	}
	db := &RqLiteDB{
		store:     store,
		logger:    logger,
		node:      node,
		tracer:    otel.GetTracerProvider().Tracer(fmt.Sprintf("partition-%d-rqlite", partition)),
		partition: partition,
		pdCache:   expirable.NewLRU[int64, runtime.ProcessDefinition](cfg.ProcDefCacheSize, nil, cfg.ProcDefCacheTTL),
	}
	queries := sql.New(db)
	db.queries = queries
	return db, nil
}

func (rq *RqLiteDB) executeStatements(ctx context.Context, statements []*proto.Statement) ([]*proto.ExecuteQueryResponse, error) {
	if len(statements) == 0 {
		return []*proto.ExecuteQueryResponse{{
			Result: &proto.ExecuteQueryResponse_E{
				E: &proto.ExecuteResult{},
			},
		}}, nil
	}
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
			DbTimeout:   (10 * time.Second).Nanoseconds(),
			Statements:  []*proto.Statement{stmts},
		},
		Timings: false,
		Level:   proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE,
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
	ctx, execSpan := rq.tracer.Start(ctx, "rqlite-exec", trace.WithAttributes(
		attribute.String(otelPkg.AttributeExec, sql),
		attribute.String(otelPkg.AttributeArgs, fmt.Sprintf("%v", args)),
	))
	defer func() {
		execSpan.End()
	}()
	result, err := rq.executeStatements(ctx, []*proto.Statement{rq.generateStatement(sql, args...)})

	if err != nil {
		execSpan.RecordError(err)
		execSpan.SetStatus(codes.Error, err.Error())
		rq.logger.Error("Error executing SQL statements")
		return nil, err
	}

	lastInsertId, rowsAffected := int64(-1), int64(-1)
	for _, r := range result {
		err := r.GetError()
		if err != "" {
			nErr := errors.New(err)
			execSpan.RecordError(nErr)
			execSpan.SetStatus(codes.Error, nErr.Error())
			return nil, nErr
		}
		lastInsertId, rowsAffected = r.GetE().LastInsertId, r.GetE().RowsAffected+rowsAffected
	}
	return rqliteResult{lastInsertId: lastInsertId, rowsAffected: rowsAffected}, nil
}

func (rq *RqLiteDB) PrepareContext(ctx context.Context, sql string) (*ssql.Stmt, error) {
	return nil, errors.New("PrepareContext not supported by rqlite")
}

func (rq *RqLiteDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	ctx, querySpan := rq.tracer.Start(ctx, "rqlite-query", trace.WithAttributes(
		attribute.String(otelPkg.AttributeQuery, query),
		attribute.String(otelPkg.AttributeArgs, fmt.Sprintf("%v", args)),
	))
	defer func() {
		querySpan.End()
	}()
	results, err := rq.queryDatabase(query, args...)
	if err != nil {
		querySpan.RecordError(err)
		querySpan.SetStatus(codes.Error, err.Error())
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

	pd, ok := rq.pdCache.Get(dbDefinition.Key)
	if ok {
		return pd, nil
	}

	var definitions bpmn20.TDefinitions
	err = xml.Unmarshal([]byte(dbDefinition.BpmnData), &definitions)
	if err != nil {
		return res, fmt.Errorf("failed to unmarshal xml data: %w", err)
	}
	err = definitions.ResolveReferences()
	if err != nil {
		return res, fmt.Errorf("failed to resolve references in definition with bpmn id%s: %w", processDefinitionId, err)
	}

	res = runtime.ProcessDefinition{
		BpmnProcessId:    dbDefinition.BpmnProcessID,
		Version:          dbDefinition.Version,
		Key:              dbDefinition.Key,
		Definitions:      definitions,
		BpmnData:         dbDefinition.BpmnData,
		BpmnResourceName: dbDefinition.BpmnResourceName,
		BpmnChecksum:     [16]byte(dbDefinition.BpmnChecksum),
	}

	rq.pdCache.Add(dbDefinition.Key, res)

	return res, nil
}

func (rq *RqLiteDB) FindProcessDefinitionByKey(ctx context.Context, processDefinitionKey int64) (runtime.ProcessDefinition, error) {
	pd, ok := rq.pdCache.Get(processDefinitionKey)
	if ok {
		return pd, nil
	}

	var res runtime.ProcessDefinition
	dbDefinition, err := rq.queries.FindProcessDefinitionByKey(ctx, processDefinitionKey)
	if err != nil {
		return res, fmt.Errorf("failed to find latest process definition: %w", err)
	}

	var definitions bpmn20.TDefinitions
	err = xml.Unmarshal([]byte(dbDefinition.BpmnData), &definitions)
	if err != nil {
		return res, fmt.Errorf("failed to unmarshal xml data: %w", err)
	}

	res = runtime.ProcessDefinition{
		BpmnProcessId:    dbDefinition.BpmnProcessID,
		Version:          dbDefinition.Version,
		Key:              dbDefinition.Key,
		Definitions:      definitions,
		BpmnData:         dbDefinition.BpmnData,
		BpmnResourceName: dbDefinition.BpmnResourceName,
		BpmnChecksum:     [16]byte(dbDefinition.BpmnChecksum),
	}

	rq.pdCache.Add(processDefinitionKey, res)

	return res, nil
}

func (rq *RqLiteDB) FindProcessDefinitionsById(ctx context.Context, processId string) ([]runtime.ProcessDefinition, error) {
	dbDefinitions, err := rq.queries.FindProcessDefinitionsByIds(ctx, processId)
	if err != nil {
		return nil, fmt.Errorf("failed to find process definitions by id: %w", err)
	}

	res := make([]runtime.ProcessDefinition, len(dbDefinitions))
	for i, def := range dbDefinitions {
		pd, ok := rq.pdCache.Get(def.Key)
		if ok {
			res[i] = pd
			continue
		}

		var definitions bpmn20.TDefinitions
		err = xml.Unmarshal([]byte(def.BpmnData), &definitions)
		if err != nil {
			return res, fmt.Errorf("failed to unmarshal xml data: %w", err)
		}
		err = definitions.ResolveReferences()
		if err != nil {
			return res, fmt.Errorf("failed to resolve references in definition %d: %w", def.Key, err)
		}
		res[i] = runtime.ProcessDefinition{
			BpmnProcessId:    def.BpmnProcessID,
			Version:          def.Version,
			Key:              def.Key,
			Definitions:      definitions,
			BpmnData:         def.BpmnData,
			BpmnResourceName: def.BpmnResourceName,
			BpmnChecksum:     [16]byte(def.BpmnChecksum),
		}

		rq.pdCache.Add(def.Key, res[i])
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

	definition, err := rq.FindProcessDefinitionByKey(ctx, dbInstance.ProcessDefinitionKey)
	if err != nil {
		return res, fmt.Errorf("failed to find process definition for process instance: %w", err)
	}

	var parentToken *runtime.ExecutionToken
	if dbInstance.ParentProcessExecutionToken.Valid {
		tokens, err := rq.queries.GetTokens(ctx, []int64{dbInstance.ParentProcessExecutionToken.Int64})
		if err != nil {
			return res, fmt.Errorf("failed to find job token %d: %w", dbInstance.ParentProcessExecutionToken.Int64, err)
		}
		if len(tokens) > 1 {
			return res, fmt.Errorf("more than one token found for parent process instance key (%d): %w", dbInstance.Key, err)
		}
		if len(tokens) == 1 {
			parentToken = &runtime.ExecutionToken{
				Key:                tokens[0].Key,
				ElementInstanceKey: tokens[0].ElementInstanceKey,
				ElementId:          tokens[0].ElementID,
				ProcessInstanceKey: tokens[0].ProcessInstanceKey,
				State:              runtime.TokenState(tokens[0].State),
			}
		}
	}

	res = runtime.ProcessInstance{
		Definition:                  &definition, //TODO: load from cache
		Key:                         dbInstance.Key,
		VariableHolder:              runtime.NewVariableHolder(nil, variables),
		CreatedAt:                   time.UnixMilli(dbInstance.CreatedAt),
		State:                       runtime.ActivityState(dbInstance.State),
		ParentProcessExecutionToken: parentToken,
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
		ParentProcessExecutionToken: ssql.NullInt64{
			Int64: ptr.Deref(processInstance.ParentProcessExecutionToken, runtime.ExecutionToken{}).Key,
			Valid: processInstance.ParentProcessExecutionToken != nil,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to save process instance %d: %w", processInstance.Key, err)
	}
	return nil
}

var _ storage.TimerStorageReader = &RqLiteDB{}

func (rq *RqLiteDB) FindTokenActiveTimerSubscriptions(ctx context.Context, tokenKey int64) ([]runtime.Timer, error) {
	dbTimers, err := rq.queries.FindTokenTimers(ctx, sql.FindTokenTimersParams{
		ExecutionToken: tokenKey,
		State:          int(runtime.TimerStateCreated),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find element timers for token %d: %w", tokenKey, err)
	}
	res := make([]runtime.Timer, len(dbTimers))
	tokensToLoad := make([]int64, len(dbTimers))
	for i, timer := range dbTimers {
		res[i] = runtime.Timer{
			ElementId:            timer.ElementID,
			ElementInstanceKey:   timer.ElementInstanceKey,
			Key:                  timer.Key,
			ProcessDefinitionKey: timer.ProcessDefinitionKey,
			ProcessInstanceKey:   timer.ProcessInstanceKey,
			TimerState:           runtime.TimerState(timer.State),
			CreatedAt:            time.UnixMilli(timer.CreatedAt),
			DueAt:                time.UnixMilli(timer.DueAt),
			Token:                runtime.ExecutionToken{Key: timer.ExecutionToken},
		}
		tokensToLoad[i] = timer.ExecutionToken
		res[i].Duration = res[i].DueAt.Sub(res[i].CreatedAt)
	}
	loadedTokens, err := rq.queries.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return nil, fmt.Errorf("failed to load timer subscriptions tokens: %w", err)
	}
	for _, token := range loadedTokens {
		// we might have the same token registered for multiple subs (event base gateway) so we have to go through whole array
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = runtime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              runtime.TokenState(token.State),
				}
			}
		}
	}
	return res, nil
}

func (rq *RqLiteDB) FindTimersTo(ctx context.Context, end time.Time) ([]runtime.Timer, error) {
	dbTimers, err := rq.queries.FindTimersInStateTillDueAt(ctx, sql.FindTimersInStateTillDueAtParams{
		State: int(runtime.TimerStateCreated),
		DueAt: end.UnixMilli(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find timers till %s in state CREATED: %w", end, err)
	}
	res := make([]runtime.Timer, len(dbTimers))
	tokensToLoad := make([]int64, len(dbTimers))
	for i, timer := range dbTimers {
		res[i] = runtime.Timer{
			ElementId:            timer.ElementID,
			ElementInstanceKey:   timer.ElementInstanceKey,
			Key:                  timer.Key,
			ProcessDefinitionKey: timer.ProcessDefinitionKey,
			ProcessInstanceKey:   timer.ProcessInstanceKey,
			TimerState:           runtime.TimerState(timer.State),
			CreatedAt:            time.UnixMilli(timer.CreatedAt),
			DueAt:                time.UnixMilli(timer.DueAt),
			Duration:             time.Millisecond * time.Duration(timer.DueAt-timer.CreatedAt),
			Token:                runtime.ExecutionToken{Key: timer.ExecutionToken},
		}
		tokensToLoad[i] = timer.ExecutionToken
	}
	loadedTokens, err := rq.queries.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return nil, fmt.Errorf("failed to load timer subscriptions tokens: %w", err)
	}
	for _, token := range loadedTokens {
		// we might have the same token registered for multiple subs (event base gateway) so we have to go through whole array
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = runtime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              runtime.TokenState(token.State),
				}
			}
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
		ElementInstanceKey:   timer.ElementInstanceKey,
		ProcessDefinitionKey: timer.ProcessDefinitionKey,
		ProcessInstanceKey:   timer.ProcessInstanceKey,
		State:                int(timer.GetState()),
		CreatedAt:            timer.CreatedAt.UnixMilli(),
		DueAt:                timer.DueAt.UnixMilli(),
		ExecutionToken:       timer.Token.Key,
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
	tokensToLoad := make([]int64, len(jobs))
	for i, job := range jobs {
		res[i] = runtime.Job{
			ElementId:          job.ElementID,
			ElementInstanceKey: job.ElementInstanceKey,
			ProcessInstanceKey: job.ProcessInstanceKey,
			Type:               job.Type,
			Key:                job.Key,
			State:              runtime.ActivityState(job.State),
			CreatedAt:          time.UnixMilli(job.CreatedAt),
			Token: runtime.ExecutionToken{
				Key: job.ExecutionToken,
			},
		}
		tokensToLoad[i] = job.ExecutionToken
	}
	tokens, err := rq.queries.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return res, fmt.Errorf("failed to find job tokens: %w", err)
	}
token:
	for _, token := range tokens {
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = runtime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              runtime.TokenState(token.State),
				}
				continue token
			}
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
	tokens, err := rq.queries.GetTokens(ctx, []int64{job.ExecutionToken})
	if err != nil {
		return res, fmt.Errorf("failed to find job token %d: %w", job.ExecutionToken, err)
	}
	if len(tokens) != 1 {
		return res, fmt.Errorf("failed to find job token %d in the database", job.ExecutionToken)
	}
	token := tokens[0]
	res = runtime.Job{
		ElementId:          job.ElementID,
		ElementInstanceKey: job.ElementInstanceKey,
		ProcessInstanceKey: job.ProcessInstanceKey,
		Key:                job.Key,
		Type:               job.Type,
		State:              runtime.ActivityState(job.State),
		CreatedAt:          time.UnixMilli(job.CreatedAt),
		Token: runtime.ExecutionToken{
			Key:                token.Key,
			ElementInstanceKey: token.ElementInstanceKey,
			ElementId:          token.ElementID,
			ProcessInstanceKey: token.ProcessInstanceKey,
			State:              runtime.TokenState(token.State),
		},
	}
	return res, nil
}

func (rq *RqLiteDB) FindJobByJobKey(ctx context.Context, jobKey int64) (runtime.Job, error) {
	var res runtime.Job
	job, err := rq.queries.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return res, fmt.Errorf("failed to find job with key %d: %w", jobKey, err)
	}
	tokens, err := rq.queries.GetTokens(ctx, []int64{job.ExecutionToken})
	if err != nil {
		return res, fmt.Errorf("failed to find job token %d: %w", job.ExecutionToken, err)
	}
	if len(tokens) != 1 {
		return res, fmt.Errorf("failed to find job token %d in the database", job.ExecutionToken)
	}
	token := tokens[0]
	res = runtime.Job{
		ElementId:          job.ElementID,
		ElementInstanceKey: job.ElementInstanceKey,
		ProcessInstanceKey: job.ProcessInstanceKey,
		Key:                job.Key,
		Type:               job.Type,
		State:              runtime.ActivityState(job.State),
		CreatedAt:          time.UnixMilli(job.CreatedAt),
		Token: runtime.ExecutionToken{
			Key:                token.Key,
			ElementInstanceKey: token.ElementInstanceKey,
			ElementId:          token.ElementID,
			ProcessInstanceKey: token.ProcessInstanceKey,
			State:              runtime.TokenState(token.State),
		},
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
	tokensToLoad := make([]int64, len(dbJobs))
	for i, job := range dbJobs {
		res[i] = runtime.Job{
			ElementId:          job.ElementID,
			ElementInstanceKey: job.ElementInstanceKey,
			ProcessInstanceKey: job.ProcessInstanceKey,
			Key:                job.Key,
			Type:               job.Type,
			State:              runtime.ActivityState(job.State),
			CreatedAt:          time.UnixMilli(job.CreatedAt),
			Token: runtime.ExecutionToken{
				Key: job.ExecutionToken,
			},
		}
		tokensToLoad[i] = job.ExecutionToken
	}
	loadedTokens, err := rq.queries.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return nil, fmt.Errorf("failed to load message subscriptions tokens: %w", err)
	}
	for _, token := range loadedTokens {
		// we might have the same token registered for multiple subs (event base gateway) so we have to go through whole array
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = runtime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              runtime.TokenState(token.State),
				}
			}
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
		Type:               job.Type,
		State:              int(job.GetState()),
		CreatedAt:          job.CreatedAt.UnixMilli(),
		Variables:          "{}", // TODO: add variables to job
		ExecutionToken:     job.Token.Key,
	})
	if err != nil {
		return fmt.Errorf("failed to save job %d: %w", job.GetKey(), err)
	}
	return nil
}

var _ storage.MessageStorageReader = &RqLiteDB{}

func (rq *RqLiteDB) FindTokenMessageSubscriptions(ctx context.Context, tokenKey int64, state runtime.ActivityState) ([]runtime.MessageSubscription, error) {
	dbMessages, err := rq.queries.FindTokenMessageSubscriptions(ctx, sql.FindTokenMessageSubscriptionsParams{
		ExecutionToken: tokenKey,
		State:          int(state),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find token message subscriptions for token %d: %w", tokenKey, err)
	}
	res := make([]runtime.MessageSubscription, len(dbMessages))
	tokensToLoad := make([]int64, len(dbMessages))
	for i, mes := range dbMessages {
		res[i] = runtime.MessageSubscription{
			ElementId:            mes.ElementID,
			ElementInstanceKey:   mes.ElementInstanceKey,
			ProcessDefinitionKey: mes.ProcessDefinitionKey,
			ProcessInstanceKey:   mes.ProcessInstanceKey,
			Name:                 mes.Name,
			MessageState:         runtime.ActivityState(mes.State),
			CreatedAt:            time.UnixMilli(mes.CreatedAt),
			Token: runtime.ExecutionToken{
				Key: mes.ExecutionToken,
			},
		}
		tokensToLoad[i] = mes.ExecutionToken
	}
	loadedTokens, err := rq.queries.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return nil, fmt.Errorf("failed to load message subscriptions tokens: %w", err)
	}
	for _, token := range loadedTokens {
		// we might have the same token registered for multiple subs (event base gateway) so we have to go through whole array
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = runtime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              runtime.TokenState(token.State),
				}
			}
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
	tokensToLoad := make([]int64, len(dbMessages))
	for i, mes := range dbMessages {
		res[i] = runtime.MessageSubscription{
			ElementId:            mes.ElementID,
			ElementInstanceKey:   mes.ElementInstanceKey,
			ProcessDefinitionKey: mes.ProcessDefinitionKey,
			ProcessInstanceKey:   mes.ProcessInstanceKey,
			Name:                 mes.Name,
			MessageState:         runtime.ActivityState(mes.State),
			CreatedAt:            time.UnixMilli(mes.CreatedAt),
			Token: runtime.ExecutionToken{
				Key: mes.ExecutionToken,
			},
		}
		tokensToLoad[i] = mes.ExecutionToken
	}
	loadedTokens, err := rq.queries.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return nil, fmt.Errorf("failed to load message subscriptions tokens: %w", err)
	}
	for _, token := range loadedTokens {
		// we might have the same token registered for multiple subs (event base gateway) so we have to go through whole array
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = runtime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              runtime.TokenState(token.State),
				}
			}
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
		ExecutionToken:       subscription.Token.Key,
		CorrelationKey:       "", // TODO: add message correlation keys into message subscription
	})
	if err != nil {
		return fmt.Errorf("failed to save message subscription %d: %w", subscription.GetKey(), err)
	}
	return nil
}

var _ storage.TokenStorageReader = &RqLiteDB{}

func (rq *RqLiteDB) GetRunningTokens(ctx context.Context) ([]runtime.ExecutionToken, error) {
	return GetActiveTokensForPartition(ctx, rq.queries, rq.partition)
}

func GetActiveTokensForPartition(ctx context.Context, db *sql.Queries, partitionId uint32) ([]runtime.ExecutionToken, error) {
	tokens, err := db.GetTokensInStateForPartition(ctx, sql.GetTokensInStateForPartitionParams{
		Partition: int64(partitionId),
		State:     int64(runtime.TokenStateRunning),
	})
	res := make([]runtime.ExecutionToken, len(tokens))
	for i, tok := range tokens {
		res[i] = runtime.ExecutionToken{
			Key:                tok.Key,
			ElementInstanceKey: tok.ElementInstanceKey,
			ElementId:          tok.ElementID,
			ProcessInstanceKey: tok.ProcessInstanceKey,
			State:              runtime.TokenState(tok.State),
		}
	}
	return res, err
}

func (rq *RqLiteDB) GetTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]runtime.ExecutionToken, error) {
	return GetTokensForProcessInstance(ctx, rq.queries, rq.partition, processInstanceKey)
}

func GetTokensForProcessInstance(ctx context.Context, db *sql.Queries, partitionId uint32, processInstanceKey int64) ([]runtime.ExecutionToken, error) {
	tokens, err := db.GetTokensForProcessInstance(ctx, sql.GetTokensForProcessInstanceParams{
		Partition:          int64(partitionId),
		ProcessInstanceKey: processInstanceKey,
	})
	res := make([]runtime.ExecutionToken, len(tokens))
	for i, tok := range tokens {
		res[i] = runtime.ExecutionToken{
			Key:                tok.Key,
			ElementInstanceKey: tok.ElementInstanceKey,
			ElementId:          tok.ElementID,
			ProcessInstanceKey: tok.ProcessInstanceKey,
			State:              runtime.TokenState(tok.State),
		}
	}
	return res, err
}

var _ storage.TokenStorageWriter = &RqLiteDB{}

func (rq *RqLiteDB) SaveToken(ctx context.Context, token runtime.ExecutionToken) error {
	return SaveToken(ctx, rq.queries, token)
}

func SaveToken(ctx context.Context, db *sql.Queries, token runtime.ExecutionToken) error {
	return db.SaveToken(ctx, sql.SaveTokenParams{
		Key:                token.Key,
		ElementInstanceKey: token.ElementInstanceKey,
		ElementID:          token.ElementId,
		ProcessInstanceKey: token.ProcessInstanceKey,
		State:              int64(token.State),
		CreatedAt:          time.Now().UnixMilli(),
	})
}

func (rq *RqLiteDB) SaveFlowElementHistory(ctx context.Context, historyItem runtime.FlowElementHistoryItem) error {
	return SaveFlowElementHistoryWith(ctx, rq.queries, historyItem)
}

func SaveFlowElementHistoryWith(ctx context.Context, db *sql.Queries, historyItem runtime.FlowElementHistoryItem) error {
	return db.SaveFlowElementHistory(
		ctx,
		sql.SaveFlowElementHistoryParams{
			historyItem.Key,
			historyItem.ElementId,
			historyItem.ProcessInstanceKey,
			historyItem.CreatedAt.UnixMilli(),
		},
	)
}

var _ storage.IncidentStorageReader = &RqLiteDB{}

func (rq *RqLiteDB) FindIncidentByKey(ctx context.Context, key int64) (runtime.Incident, error) {
	return FindIncidentByKey(ctx, rq.queries, key)
}

func FindIncidentByKey(ctx context.Context, db *sql.Queries, key int64) (runtime.Incident, error) {
	incident, err := db.FindIncidentByKey(ctx, key)
	if err != nil {
		return runtime.Incident{}, err
	}

	tokens, err := db.GetTokens(ctx, []int64{incident.ExecutionToken})
	if len(tokens) == 0 {
		err = errors.Join(err, errors.New("no incidents found"))
	}
	token := tokens[0]
	return runtime.Incident{
		Key:                incident.Key,
		ElementInstanceKey: incident.ElementInstanceKey,
		ElementId:          incident.ElementID,
		ProcessInstanceKey: incident.ProcessInstanceKey,
		Message:            incident.Message,
		CreatedAt:          time.UnixMilli(incident.CreatedAt),
		ResolvedAt: func() *time.Time {
			if incident.ResolvedAt.Valid {
				t := time.UnixMilli(incident.ResolvedAt.Int64)
				return &t
			}
			return nil
		}(),
		Token: runtime.ExecutionToken{
			Key:                token.Key,
			ElementInstanceKey: token.ElementInstanceKey,
			ElementId:          token.ElementID,
			ProcessInstanceKey: token.ProcessInstanceKey,
			State:              runtime.TokenState(token.State),
		},
	}, nil
}

func (rq *RqLiteDB) FindIncidentsByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]runtime.Incident, error) {
	return FindIncidentsByProcessInstanceKey(ctx, rq.queries, processInstanceKey)
}

func FindIncidentsByProcessInstanceKey(ctx context.Context, db *sql.Queries, processInstanceKey int64) ([]runtime.Incident, error) {
	incidents, err := db.FindIncidentsByProcessInstanceKey(ctx, processInstanceKey)
	if err != nil {
		return nil, err
	}
	res := make([]runtime.Incident, len(incidents))

	tokensToLoad := make([]int64, len(incidents))
	for i, incident := range incidents {
		res[i] = runtime.Incident{
			Key:                incident.Key,
			ElementInstanceKey: incident.ElementInstanceKey,
			ElementId:          incident.ElementID,
			ProcessInstanceKey: incident.ProcessInstanceKey,
			Message:            incident.Message,
		}

		tokensToLoad[i] = incident.ExecutionToken
	}

	tokens, err := db.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return res, fmt.Errorf("failed to find job tokens: %w", err)
	}
token:
	for _, token := range tokens {
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = runtime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              runtime.TokenState(token.State),
				}
				continue token
			}
		}
	}
	return res, nil
}

var _ storage.IncidentStorageWriter = &RqLiteDB{}

func (rq *RqLiteDB) SaveIncident(ctx context.Context, incident runtime.Incident) error {
	return SaveIncidentWith(ctx, rq.queries, incident)
}

func SaveIncidentWith(ctx context.Context, db *sql.Queries, incident runtime.Incident) error {
	return db.SaveIncident(ctx, sql.SaveIncidentParams{
		Key:                incident.Key,
		ElementInstanceKey: incident.ElementInstanceKey,
		ElementID:          incident.ElementId,
		ProcessInstanceKey: incident.ProcessInstanceKey,
		Message:            incident.Message,
		CreatedAt:          incident.CreatedAt.UnixMilli(),
		ResolvedAt: ssql.NullInt64{
			Int64: ptr.Deref(incident.ResolvedAt, time.Now()).UnixMilli(),
			Valid: incident.ResolvedAt != nil,
		},
		ExecutionToken: incident.Token.Key,
	})
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
	ctx, execSpan := b.db.tracer.Start(ctx, "rqlite-batch", trace.WithAttributes(
		attribute.String(otelPkg.AttributeExec, fmt.Sprintf("%v", b.stmtToRun)),
	))
	defer func() {
		execSpan.End()
	}()
	_, err := b.db.executeStatements(ctx, b.stmtToRun)
	if err != nil {
	}
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

var _ storage.TokenStorageWriter = &RqLiteDBBatch{}

func (b *RqLiteDBBatch) SaveToken(ctx context.Context, token runtime.ExecutionToken) error {
	return SaveToken(ctx, b.queries, token)
}

func (b *RqLiteDBBatch) SaveFlowElementHistory(ctx context.Context, historyItem runtime.FlowElementHistoryItem) error {
	return SaveFlowElementHistoryWith(ctx, b.queries, historyItem)
}

var _ storage.IncidentStorageWriter = &RqLiteDBBatch{}

func (b *RqLiteDBBatch) SaveIncident(ctx context.Context, incident runtime.Incident) error {
	return SaveIncidentWith(ctx, b.queries, incident)
}
