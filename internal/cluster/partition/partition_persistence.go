package partition

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/internal/appcontext"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	grpccode "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	dmnruntime "github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"

	ssql "database/sql"

	"github.com/bwmarrin/snowflake"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/golang-lru/v2/expirable"
	zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/config"
	otelPkg "github.com/pbinitiative/zenbpm/internal/otel"
	"github.com/pbinitiative/zenbpm/internal/profile"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/store"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type DB struct {
	Store                  *store.Store
	Queries                *sql.Queries
	logger                 hclog.Logger
	node                   *snowflake.Node
	Partition              uint32
	tracer                 trace.Tracer
	pdCache                *expirable.LRU[int64, bpmnruntime.ProcessDefinition]
	ddCache                *expirable.LRU[int64, dmnruntime.DecisionDefinition]
	client                 *client.ClientManager
	zenState               func() state.Cluster
	historyDeleteThreshold int
}

// GenerateId implements storage.Storage.
func (rq *DB) GenerateId() int64 {
	return rq.node.Generate().Int64()
}

type Querier interface {
	getQueries() *sql.Queries
	getReadDB() *DB
	getLogger() hclog.Logger
}

func (d *DB) getQueries() *sql.Queries {
	return d.Queries
}

func (d *DB) getReadDB() *DB {
	return d
}

func (d *DB) getLogger() hclog.Logger {
	return d.logger
}

func NewDB(store *store.Store, partition uint32, logger hclog.Logger, cfg config.Persistence, client *client.ClientManager, zenState func() state.Cluster) (*DB, error) {
	node, err := snowflake.NewNode(int64(partition))
	if err != nil {
		return nil, fmt.Errorf("failed to create snowflake node for partition %d: %w", partition, err)
	}
	db := &DB{
		Store:                  store,
		logger:                 logger,
		node:                   node,
		tracer:                 otel.GetTracerProvider().Tracer(fmt.Sprintf("partition-%d-rqlite", partition)),
		Partition:              partition,
		pdCache:                expirable.NewLRU[int64, bpmnruntime.ProcessDefinition](cfg.ProcDefCacheSize, nil, time.Duration(cfg.ProcDefCacheTTL)),
		ddCache:                expirable.NewLRU[int64, dmnruntime.DecisionDefinition](cfg.DecDefCacheSize, nil, time.Duration(cfg.DecDefCacheTTL)),
		client:                 client,
		zenState:               zenState,
		historyDeleteThreshold: 1000,
	}
	queries := sql.New(db)
	db.Queries = queries

	go db.scheduleDataCleanup()
	return db, nil
}

func (rq *DB) scheduleDataCleanup() {
	t := time.NewTicker(30 * time.Second)
	for range t.C {
		err := rq.dataCleanup(time.Now())
		if err != nil {
			rq.logger.Error(fmt.Sprintf("Error while performing data cleanup: %s", err))
		}
	}
}

func (rq *DB) dataCleanup(currTime time.Time) error {
	ctx := context.Background()
	processes, _ := rq.Queries.FindInactiveInstancesToDelete(ctx, ssql.NullInt64{
		Int64: currTime.Unix(),
		Valid: true,
	})
	var err error
	if len(processes) > rq.historyDeleteThreshold {
		err = errors.Join(err, rq.Queries.DeleteFlowElementHistory(ctx, processes))
		err = errors.Join(err, rq.Queries.DeleteProcessInstancesTokens(ctx, processes))
		err = errors.Join(err, rq.Queries.DeleteProcessInstancesJobs(ctx, processes))
		err = errors.Join(err, rq.Queries.DeleteProcessInstancesTimers(ctx, processes))
		err = errors.Join(err, rq.Queries.DeleteProcessInstancesMessageSubscriptions(ctx, processes))
		err = errors.Join(err, rq.Queries.DeleteProcessInstancesIncidents(ctx, processes))
		err = errors.Join(err, rq.Queries.DeleteProcessInstances(ctx, processes))
	}
	return err
}

func (rq *DB) ExecuteStatements(ctx context.Context, statements []*proto.Statement) ([]*proto.ExecuteQueryResponse, error) {
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

	results, resultsErr := rq.Store.Execute(er)

	if resultsErr != nil {
		if ctx.Err() == context.DeadlineExceeded {
			rq.logger.Error("Context deadline exceeded for statement", "err", resultsErr)
		}
		rq.logger.Error("Error executing SQL statements", "err", resultsErr)
		return nil, resultsErr
	}
	return results, nil
}

func (rq *DB) generateStatement(sql string, parameters ...interface{}) *proto.Statement {
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

func (rq *DB) queryDatabase(query string, parameters ...interface{}) ([]*proto.QueryRows, error) {
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

	results, resultsErr := rq.Store.Query(qr)
	if resultsErr != nil {
		rq.logger.Error("Error executing SQL statements", "err", resultsErr)
		return nil, resultsErr
	}
	if len(results) == 1 && results[0].Error != "" {
		err := fmt.Errorf("error executing SQL statement %s %+v: %s", query, parameters, results[0].Error)
		rq.logger.Error(err.Error())
		return nil, err
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

func (rq *DB) ExecContext(ctx context.Context, sql string, args ...interface{}) (ssql.Result, error) {
	ctx, execSpan := rq.tracer.Start(ctx, "rqlite-exec", trace.WithAttributes(
		attribute.String(otelPkg.AttributeExec, sql),
		attribute.String(otelPkg.AttributeArgs, fmt.Sprintf("%v", args)),
	))
	defer func() {
		execSpan.End()
	}()
	result, err := rq.ExecuteStatements(ctx, []*proto.Statement{rq.generateStatement(sql, args...)})

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

func (rq *DB) PrepareContext(ctx context.Context, sql string) (*ssql.Stmt, error) {
	return nil, errors.New("PrepareContext not supported by rqlite")
}

func (rq *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
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

func (rq *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
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

var _ storage.Storage = &DB{}

func (rq *DB) NewBatch() storage.Batch {
	batch := &DBBatch{
		db:               rq,
		stmtToRun:        make([]*proto.Statement, 0, 10),
		postFlushActions: make([]func(), 0, 5),
		preFlushActions:  make([]func() error, 0, 5),
		logger:           rq.logger,
	}
	queries := sql.New(batch)
	batch.queries = queries
	return batch
}

var _ storage.DecisionStorageReader = &DB{}

func (rq *DB) GetLatestDecisionById(ctx context.Context, decisionId string) (dmnruntime.Decision, error) {
	var res dmnruntime.Decision
	dbDecision, err := rq.Queries.FindLatestDecisionById(ctx, decisionId)
	if err != nil {
		return res, fmt.Errorf("failed to find latest decision by id: %w", err)
	}

	res = dmnruntime.Decision{
		Version:               dbDecision.Version,
		Id:                    dbDecision.DecisionID,
		VersionTag:            dbDecision.VersionTag,
		DecisionDefinitionId:  dbDecision.DecisionDefinitionID,
		DecisionDefinitionKey: dbDecision.DecisionDefinitionKey,
	}

	return res, nil
}

func (rq *DB) GetDecisionsById(ctx context.Context, decisionId string) ([]dmnruntime.Decision, error) {
	dbDecisions, err := rq.Queries.FindDecisionsById(ctx, decisionId)
	if err != nil {
		return nil, fmt.Errorf("failed to find decisions by id: %w", err)
	}

	res := make([]dmnruntime.Decision, len(dbDecisions))
	for i, dec := range dbDecisions {
		res[i] = dmnruntime.Decision{
			Version:               dec.Version,
			Id:                    dec.DecisionID,
			VersionTag:            dec.VersionTag,
			DecisionDefinitionId:  dec.DecisionDefinitionID,
			DecisionDefinitionKey: dec.DecisionDefinitionKey,
		}
	}
	return res, nil
}

func (rq *DB) GetLatestDecisionByIdAndVersionTag(ctx context.Context, decisionId string, versionTag string) (dmnruntime.Decision, error) {
	var res dmnruntime.Decision
	dbDecision, err := rq.Queries.FindLatestDecisionByIdAndVersionTag(ctx,
		sql.FindLatestDecisionByIdAndVersionTagParams{
			DecisionID: decisionId,
			VersionTag: versionTag,
		},
	)
	if err != nil {
		return res, fmt.Errorf("failed to find latest decision by id and version tag: %w", err)
	}

	res = dmnruntime.Decision{
		Version:               dbDecision.Version,
		Id:                    dbDecision.DecisionID,
		VersionTag:            dbDecision.VersionTag,
		DecisionDefinitionId:  dbDecision.DecisionDefinitionID,
		DecisionDefinitionKey: dbDecision.DecisionDefinitionKey,
	}

	return res, nil
}

func (rq *DB) GetLatestDecisionByIdAndDecisionDefinitionId(ctx context.Context, decisionId string, decisionDefinitionId string) (dmnruntime.Decision, error) {
	var res dmnruntime.Decision
	dbDecision, err := rq.Queries.FindLatestDecisionByIdAndDecisionDefinitionId(ctx,
		sql.FindLatestDecisionByIdAndDecisionDefinitionIdParams{
			DecisionID:           decisionId,
			DecisionDefinitionID: decisionDefinitionId,
		},
	)
	if err != nil {
		return res, fmt.Errorf("failed to find latest decision by id and decisionDefinitionId: %w", err)
	}

	res = dmnruntime.Decision{
		Version:               dbDecision.Version,
		Id:                    dbDecision.DecisionID,
		VersionTag:            dbDecision.VersionTag,
		DecisionDefinitionId:  dbDecision.DecisionDefinitionID,
		DecisionDefinitionKey: dbDecision.DecisionDefinitionKey,
	}

	return res, nil
}

func (rq *DB) GetDecisionByIdAndDecisionDefinitionKey(ctx context.Context, decisionId string, decisionDefinitionKey int64) (dmnruntime.Decision, error) {
	var res dmnruntime.Decision
	dbDecision, err := rq.Queries.FindDecisionByIdAndDecisionDefinitionKey(
		ctx,
		sql.FindDecisionByIdAndDecisionDefinitionKeyParams{
			DecisionDefinitionKey: decisionDefinitionKey,
			DecisionID:            decisionId,
		})
	if err != nil {
		return res, fmt.Errorf("failed to find decision by key: %w", err)
	}

	res = dmnruntime.Decision{
		Version:               dbDecision.Version,
		Id:                    dbDecision.DecisionID,
		VersionTag:            dbDecision.VersionTag,
		DecisionDefinitionId:  dbDecision.DecisionDefinitionID,
		DecisionDefinitionKey: dbDecision.DecisionDefinitionKey,
	}

	return res, nil
}

var _ storage.DecisionStorageWriter = &DB{}

func (rq *DB) SaveDecision(ctx context.Context, decision dmnruntime.Decision) error {
	return SaveDecisionWith(ctx, rq.Queries, decision)
}

func SaveDecisionWith(ctx context.Context, db *sql.Queries, decision dmnruntime.Decision) error {
	err := db.SaveDecision(ctx, sql.SaveDecisionParams{
		Version:               decision.Version,
		DecisionID:            decision.Id,
		VersionTag:            decision.VersionTag,
		DecisionDefinitionID:  decision.DecisionDefinitionId,
		DecisionDefinitionKey: decision.DecisionDefinitionKey,
	})
	if err != nil {
		return fmt.Errorf("failed to save decision: %w", err)
	}
	return nil
}

var _ storage.DecisionDefinitionStorageWriter = &DB{}

func (rq *DB) SaveDecisionDefinition(ctx context.Context, definition dmnruntime.DecisionDefinition) error {
	return SaveDecisionDefinitionWith(ctx, rq.Queries, definition)
}

func SaveDecisionDefinitionWith(ctx context.Context, db *sql.Queries, definition dmnruntime.DecisionDefinition) error {
	err := db.SaveDecisionDefinition(ctx, sql.SaveDecisionDefinitionParams{
		DmnID:           definition.Id,
		Key:             definition.Key,
		Version:         definition.Version,
		DmnData:         string(definition.DmnData),
		DmnChecksum:     definition.DmnChecksum[:],
		DmnResourceName: definition.DmnResourceName,
	})
	if err != nil {
		return fmt.Errorf("failed to save decision definition: %w", err)
	}
	return nil
}

var _ storage.DecisionDefinitionStorageReader = &DB{}

func (rq *DB) FindLatestDecisionDefinitionById(ctx context.Context, decisionDefinitionId string) (dmnruntime.DecisionDefinition, error) {
	var res dmnruntime.DecisionDefinition
	dbDefinition, err := rq.Queries.FindLatestDecisionDefinitionById(ctx, decisionDefinitionId)
	if err != nil {
		return res, fmt.Errorf("failed to find latest decision definition: %w", err)
	}

	dd, ok := rq.ddCache.Get(dbDefinition.Key)
	if ok {
		return dd, nil
	}

	var definitions dmn.TDefinitions
	err = xml.Unmarshal([]byte(dbDefinition.DmnData), &definitions)
	if err != nil {
		return res, fmt.Errorf("failed to unmarshal xml data: %w", err)
	}

	res = dmnruntime.DecisionDefinition{
		Id:              dbDefinition.DmnID,
		Version:         dbDefinition.Version,
		Key:             dbDefinition.Key,
		Definitions:     definitions,
		DmnData:         []byte(dbDefinition.DmnData),
		DmnResourceName: dbDefinition.DmnResourceName,
		DmnChecksum:     [16]byte(dbDefinition.DmnChecksum),
	}

	rq.ddCache.Add(dbDefinition.Key, res)

	return res, nil
}

func (rq *DB) FindDecisionDefinitionByKey(ctx context.Context, decisionDefinitionKey int64) (dmnruntime.DecisionDefinition, error) {
	dd, ok := rq.ddCache.Get(decisionDefinitionKey)
	if ok {
		return dd, nil
	}

	var res dmnruntime.DecisionDefinition
	dbDefinition, err := rq.Queries.FindDecisionDefinitionByKey(ctx, decisionDefinitionKey)
	if err != nil {
		return res, fmt.Errorf("failed to find latest decision definition: %w", err)
	}

	var definitions dmn.TDefinitions
	err = xml.Unmarshal([]byte(dbDefinition.DmnData), &definitions)
	if err != nil {
		return res, fmt.Errorf("failed to unmarshal xml data: %w", err)
	}

	res = dmnruntime.DecisionDefinition{
		Id:              dbDefinition.DmnID,
		Version:         dbDefinition.Version,
		Key:             dbDefinition.Key,
		Definitions:     definitions,
		DmnData:         []byte(dbDefinition.DmnData),
		DmnResourceName: dbDefinition.DmnResourceName,
		DmnChecksum:     [16]byte(dbDefinition.DmnChecksum),
	}

	rq.ddCache.Add(decisionDefinitionKey, res)

	return res, nil
}

func (rq *DB) FindDecisionDefinitionsById(ctx context.Context, decisionDefinitionId string) ([]dmnruntime.DecisionDefinition, error) {
	dbDefinitions, err := rq.Queries.FindDecisionDefinitionsById(ctx, decisionDefinitionId)
	if err != nil {
		return nil, fmt.Errorf("failed to find decision definitions by id: %w", err)
	}

	res := make([]dmnruntime.DecisionDefinition, len(dbDefinitions))
	for i, def := range dbDefinitions {
		dd, ok := rq.ddCache.Get(def.Key)
		if ok {
			res[i] = dd
			continue
		}

		var definitions dmn.TDefinitions
		err = xml.Unmarshal([]byte(def.DmnData), &definitions)
		if err != nil {
			return res, fmt.Errorf("failed to unmarshal xml data: %w", err)
		}
		res[i] = dmnruntime.DecisionDefinition{
			Id:              def.DmnID,
			Version:         def.Version,
			Key:             def.Key,
			Definitions:     definitions,
			DmnData:         []byte(def.DmnData),
			DmnResourceName: def.DmnResourceName,
			DmnChecksum:     [16]byte(def.DmnChecksum),
		}

		rq.ddCache.Add(def.Key, res[i])
	}
	return res, nil
}

var _ storage.ProcessDefinitionStorageReader = &DB{}

func (rq *DB) FindLatestProcessDefinitionById(ctx context.Context, processDefinitionId string) (bpmnruntime.ProcessDefinition, error) {
	var res bpmnruntime.ProcessDefinition
	dbDefinition, err := rq.Queries.FindLatestProcessDefinitionById(ctx, processDefinitionId)
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

	res = bpmnruntime.ProcessDefinition{
		BpmnProcessId:    dbDefinition.BpmnProcessID,
		Version:          int32(dbDefinition.Version),
		Key:              dbDefinition.Key,
		Definitions:      definitions,
		BpmnData:         dbDefinition.BpmnData,
		BpmnResourceName: dbDefinition.BpmnResourceName,
		BpmnChecksum:     [16]byte(dbDefinition.BpmnChecksum),
	}

	rq.pdCache.Add(dbDefinition.Key, res)

	return res, nil
}

func (rq *DB) FindProcessDefinitionByKey(ctx context.Context, processDefinitionKey int64) (bpmnruntime.ProcessDefinition, error) {
	pd, ok := rq.pdCache.Get(processDefinitionKey)
	if ok {
		return pd, nil
	}

	var res bpmnruntime.ProcessDefinition
	dbDefinition, err := rq.Queries.FindProcessDefinitionByKey(ctx, processDefinitionKey)
	if err != nil {
		return res, fmt.Errorf("failed to find latest process definition: %w", err)
	}

	var definitions bpmn20.TDefinitions
	err = xml.Unmarshal([]byte(dbDefinition.BpmnData), &definitions)
	if err != nil {
		return res, fmt.Errorf("failed to unmarshal xml data: %w", err)
	}

	res = bpmnruntime.ProcessDefinition{
		BpmnProcessId:    dbDefinition.BpmnProcessID,
		Version:          int32(dbDefinition.Version),
		Key:              dbDefinition.Key,
		Definitions:      definitions,
		BpmnData:         dbDefinition.BpmnData,
		BpmnResourceName: dbDefinition.BpmnResourceName,
		BpmnChecksum:     [16]byte(dbDefinition.BpmnChecksum),
	}

	rq.pdCache.Add(processDefinitionKey, res)

	return res, nil
}

func (rq *DB) FindProcessDefinitionsById(ctx context.Context, processId string) ([]bpmnruntime.ProcessDefinition, error) {
	dbDefinitions, err := rq.Queries.FindProcessDefinitionsById(ctx, processId)
	if err != nil {
		return nil, fmt.Errorf("failed to find process definitions by id: %w", err)
	}

	res := make([]bpmnruntime.ProcessDefinition, len(dbDefinitions))
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

		res[i] = bpmnruntime.ProcessDefinition{
			BpmnProcessId:    def.BpmnProcessID,
			Version:          int32(def.Version),
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

var _ storage.ProcessDefinitionStorageWriter = &DB{}

func (rq *DB) SaveProcessDefinition(ctx context.Context, definition bpmnruntime.ProcessDefinition) error {
	return SaveProcessDefinitionWith(ctx, rq.Queries, definition)
}

func SaveProcessDefinitionWith(ctx context.Context, db *sql.Queries, definition bpmnruntime.ProcessDefinition) error {
	err := db.SaveProcessDefinition(ctx, sql.SaveProcessDefinitionParams{
		Key:              definition.Key,
		Version:          int64(definition.Version),
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

var _ storage.ProcessInstanceStorageReader = &DB{}

func (rq *DB) FindProcessInstanceByKey(ctx context.Context, processInstanceKey int64) (bpmnruntime.ProcessInstance, error) {
	dbInstance, err := rq.Queries.GetProcessInstance(ctx, processInstanceKey)
	if err != nil {
		return bpmnruntime.ProcessInstance{}, fmt.Errorf("failed to find process instance by key: %w", err)
	}

	return rq.inflateProcessInstance(ctx, rq.Queries, dbInstance)
}

func (rq *DB) inflateProcessInstance(ctx context.Context, db *sql.Queries, dbInstance sql.ProcessInstance) (bpmnruntime.ProcessInstance, error) {
	var res bpmnruntime.ProcessInstance
	variables := map[string]any{}
	err := json.Unmarshal([]byte(dbInstance.Variables), &variables)
	if err != nil {
		return bpmnruntime.ProcessInstance{}, fmt.Errorf("failed to unmarshal variables: %w", err)
	}

	definition, err := rq.FindProcessDefinitionByKey(ctx, dbInstance.ProcessDefinitionKey)
	if err != nil {
		return res, fmt.Errorf("failed to find process definition for process instance: %w", err)
	}

	var parentToken *bpmnruntime.ExecutionToken
	if dbInstance.ParentProcessExecutionToken.Valid {
		tokens, err := rq.Queries.GetTokens(ctx, []int64{dbInstance.ParentProcessExecutionToken.Int64})
		if err != nil {
			return res, fmt.Errorf("failed to find job token %d: %w", dbInstance.ParentProcessExecutionToken.Int64, err)
		}
		if len(tokens) > 1 {
			return res, fmt.Errorf("more than one token found for parent process instance key (%d): %w", dbInstance.Key, err)
		}
		if len(tokens) == 1 {
			parentToken = &bpmnruntime.ExecutionToken{
				Key:                tokens[0].Key,
				ElementInstanceKey: tokens[0].ElementInstanceKey,
				ElementId:          tokens[0].ElementID,
				ProcessInstanceKey: tokens[0].ProcessInstanceKey,
				State:              bpmnruntime.TokenState(tokens[0].State),
			}
		}
	}

	res = bpmnruntime.ProcessInstance{
		Definition:                  &definition,
		Key:                         dbInstance.Key,
		VariableHolder:              bpmnruntime.NewVariableHolder(nil, variables),
		CreatedAt:                   time.UnixMilli(dbInstance.CreatedAt),
		State:                       bpmnruntime.ActivityState(dbInstance.State),
		ParentProcessExecutionToken: parentToken,
	}

	return res, nil
}

func (rq *DB) FindProcessInstanceByParentExecutionTokenKey(ctx context.Context, parentExecutionTokenKey int64) ([]bpmnruntime.ProcessInstance, error) {
	var res []bpmnruntime.ProcessInstance
	dbInstances, err := rq.Queries.FindProcessByParentExecutionToken(ctx, ssql.NullInt64{
		Int64: parentExecutionTokenKey,
		Valid: true,
	})
	if err != nil {
		return res, fmt.Errorf("failed to find process instance by key: %w", err)
	}

	for _, dbInstance := range dbInstances {
		inst, err := rq.inflateProcessInstance(ctx, rq.Queries, dbInstance)
		if err != nil {
			return res, fmt.Errorf("failed to inflate process instance: %w", err)
		}
		res = append(res, inst)
	}
	return res, nil
}

var _ storage.ProcessInstanceStorageWriter = &DB{}

func (rq *DB) SaveProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) error {
	return SaveProcessInstanceWith(ctx, rq, processInstance)
}

func SaveProcessInstanceWith(ctx context.Context, db Querier, processInstance bpmnruntime.ProcessInstance) error {
	varStr, err := json.Marshal(processInstance.VariableHolder.Variables())
	if err != nil {
		return fmt.Errorf("failed to marshal variables for instance %d: %w", processInstance.Key, err)
	}
	err = db.getQueries().SaveProcessInstance(ctx, sql.SaveProcessInstanceParams{
		Key:                  processInstance.Key,
		ProcessDefinitionKey: processInstance.Definition.Key,
		CreatedAt:            processInstance.CreatedAt.UnixMilli(),
		State:                int64(processInstance.State),
		Variables:            string(varStr),
		ParentProcessExecutionToken: ssql.NullInt64{
			Int64: ptr.Deref(processInstance.ParentProcessExecutionToken, bpmnruntime.ExecutionToken{}).Key,
			Valid: processInstance.ParentProcessExecutionToken != nil,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to save process instance %d: %w", processInstance.Key, err)
	}
	if processInstance.State == bpmnruntime.ActivityStateReady {
		historyTTL, found := appcontext.HistoryTTLFromContext(ctx)
		if !found {
			return nil
		}
		err = db.getQueries().SetProcessInstanceTTL(ctx, sql.SetProcessInstanceTTLParams{
			HistoryTTLSec: ssql.NullInt64{Valid: true, Int64: int64(historyTTL.Seconds())},
			Key:           processInstance.Key,
		})
		if err != nil {
			return fmt.Errorf("failed to set process instance TTL %d: %w", processInstance.Key, err)
		}
	}
	if processInstance.State == bpmnruntime.ActivityStateCompleted ||
		processInstance.State == bpmnruntime.ActivityStateTerminated {
		pi, err := db.getReadDB().Queries.GetProcessInstance(ctx, processInstance.Key)
		if err != nil {
			return fmt.Errorf("failed to read process instance for history processing: %w", err)
		}
		if !pi.HistoryTtlSec.Valid {
			return nil
		}
		toDeleteTime := time.Now().Add(time.Duration(pi.HistoryTtlSec.Int64) * time.Second)
		err = db.getQueries().SetProcessInstanceTTL(ctx, sql.SetProcessInstanceTTLParams{
			HistoryDeleteSec: ssql.NullInt64{Valid: true, Int64: toDeleteTime.Unix()},
			Key:              processInstance.Key,
		})
		if err != nil {
			return fmt.Errorf("failed to set process instance deletion mark: %w", err)
		}
	}
	return nil
}

var _ storage.TimerStorageReader = &DB{}

func (rq *DB) FindTokenActiveTimerSubscriptions(ctx context.Context, tokenKey int64) ([]bpmnruntime.Timer, error) {
	dbTimers, err := rq.Queries.FindTokenTimers(ctx, sql.FindTokenTimersParams{
		ExecutionToken: tokenKey,
		State:          int64(bpmnruntime.TimerStateCreated),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find element timers for token %d: %w", tokenKey, err)
	}

	return rq.inflateTimers(ctx, dbTimers)
}

func (rq *DB) FindProcessInstanceTimers(ctx context.Context, processInstanceKey int64, state bpmnruntime.TimerState) ([]bpmnruntime.Timer, error) {
	dbTimers, err := rq.Queries.FindProcessInstanceTimersInState(ctx, sql.FindProcessInstanceTimersInStateParams{
		ProcessInstanceKey: processInstanceKey,
		State:              int64(state),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find element timers for process instance %d: %w", processInstanceKey, err)
	}

	return rq.inflateTimers(ctx, dbTimers)

}

func (rq *DB) FindTimersTo(ctx context.Context, end time.Time) ([]bpmnruntime.Timer, error) {
	dbTimers, err := rq.Queries.FindTimersInStateTillDueAt(ctx, sql.FindTimersInStateTillDueAtParams{
		State: int64(bpmnruntime.TimerStateCreated),
		DueAt: end.UnixMilli(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find timers till %s in state CREATED: %w", end, err)
	}

	return rq.inflateTimers(ctx, dbTimers)
}

func (rq *DB) inflateTimers(ctx context.Context, dbTimers []sql.Timer) ([]bpmnruntime.Timer, error) {
	res := make([]bpmnruntime.Timer, len(dbTimers))
	tokensToLoad := make([]int64, len(dbTimers))
	for i, timer := range dbTimers {
		res[i] = bpmnruntime.Timer{
			ElementId:            timer.ElementID,
			ElementInstanceKey:   timer.ElementInstanceKey,
			Key:                  timer.Key,
			ProcessDefinitionKey: timer.ProcessDefinitionKey,
			ProcessInstanceKey:   timer.ProcessInstanceKey,
			TimerState:           bpmnruntime.TimerState(timer.State),
			CreatedAt:            time.UnixMilli(timer.CreatedAt),
			DueAt:                time.UnixMilli(timer.DueAt),
			Token:                bpmnruntime.ExecutionToken{Key: timer.ExecutionToken},
		}
		tokensToLoad[i] = timer.ExecutionToken
		res[i].Duration = res[i].DueAt.Sub(res[i].CreatedAt)
	}
	loadedTokens, err := rq.Queries.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return nil, fmt.Errorf("failed to load timer subscriptions tokens: %w", err)
	}
	for _, token := range loadedTokens {
		// we might have the same token registered for multiple subs (event base gateway) so we have to go through whole array
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = bpmnruntime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              bpmnruntime.TokenState(token.State),
				}
			}
		}
	}
	return res, nil
}

var _ storage.TimerStorageWriter = &DB{}

func (rq *DB) SaveTimer(ctx context.Context, timer bpmnruntime.Timer) error {
	return SaveTimerWith(ctx, rq.Queries, timer)
}

func SaveTimerWith(ctx context.Context, db *sql.Queries, timer bpmnruntime.Timer) error {
	err := db.SaveTimer(ctx, sql.SaveTimerParams{
		Key:                  timer.GetKey(),
		ElementID:            timer.ElementId,
		ElementInstanceKey:   timer.ElementInstanceKey,
		ProcessDefinitionKey: timer.ProcessDefinitionKey,
		ProcessInstanceKey:   timer.ProcessInstanceKey,
		State:                int64(timer.GetState()),
		CreatedAt:            timer.CreatedAt.UnixMilli(),
		DueAt:                timer.DueAt.UnixMilli(),
		ExecutionToken:       timer.Token.Key,
	})
	if err != nil {
		return fmt.Errorf("failed to save timer %d: %w", timer.GetKey(), err)
	}
	return nil
}

var _ storage.JobStorageReader = &DB{}

func (rq *DB) FindActiveJobsByType(ctx context.Context, jobType string) ([]bpmnruntime.Job, error) {
	jobs, err := rq.Queries.FindActiveJobsByType(ctx, jobType)
	if err != nil {
		return nil, fmt.Errorf("failed to find active jobs for type %s: %w", jobType, err)
	}
	res := make([]bpmnruntime.Job, len(jobs))
	tokensToLoad := make([]int64, len(jobs))
	for i, job := range jobs {
		res[i] = bpmnruntime.Job{
			ElementId:          job.ElementID,
			ElementInstanceKey: job.ElementInstanceKey,
			ProcessInstanceKey: job.ProcessInstanceKey,
			Type:               job.Type,
			Key:                job.Key,
			State:              bpmnruntime.ActivityState(job.State),
			CreatedAt:          time.UnixMilli(job.CreatedAt),
			Token: bpmnruntime.ExecutionToken{
				Key: job.ExecutionToken,
			},
		}
		tokensToLoad[i] = job.ExecutionToken
	}
	tokens, err := rq.Queries.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return res, fmt.Errorf("failed to find job tokens: %w", err)
	}
token:
	for _, token := range tokens {
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = bpmnruntime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              bpmnruntime.TokenState(token.State),
				}
				continue token
			}
		}
	}
	return res, nil
}

func (rq *DB) FindJobByElementID(ctx context.Context, processInstanceKey int64, elementID string) (bpmnruntime.Job, error) {
	var res bpmnruntime.Job
	job, err := rq.Queries.FindJobByElementId(ctx, sql.FindJobByElementIdParams{
		ElementID:          elementID,
		ProcessInstanceKey: processInstanceKey,
	})
	if err != nil {
		return res, fmt.Errorf("failed to find job for elementId %s and process instance key %d: %w", elementID, processInstanceKey, err)
	}
	tokens, err := rq.Queries.GetTokens(ctx, []int64{job.ExecutionToken})
	if err != nil {
		return res, fmt.Errorf("failed to find job token %d: %w", job.ExecutionToken, err)
	}
	if len(tokens) != 1 {
		return res, fmt.Errorf("failed to find job token %d in the database", job.ExecutionToken)
	}
	token := tokens[0]
	res = bpmnruntime.Job{
		ElementId:          job.ElementID,
		ElementInstanceKey: job.ElementInstanceKey,
		ProcessInstanceKey: job.ProcessInstanceKey,
		Key:                job.Key,
		Type:               job.Type,
		State:              bpmnruntime.ActivityState(job.State),
		CreatedAt:          time.UnixMilli(job.CreatedAt),
		Token: bpmnruntime.ExecutionToken{
			Key:                token.Key,
			ElementInstanceKey: token.ElementInstanceKey,
			ElementId:          token.ElementID,
			ProcessInstanceKey: token.ProcessInstanceKey,
			State:              bpmnruntime.TokenState(token.State),
		},
	}
	return res, nil
}

func (rq *DB) FindJobByJobKey(ctx context.Context, jobKey int64) (bpmnruntime.Job, error) {
	var res bpmnruntime.Job
	job, err := rq.Queries.FindJobByJobKey(ctx, jobKey)
	if err != nil {
		return res, fmt.Errorf("failed to find job with key %d: %w", jobKey, err)
	}
	tokens, err := rq.Queries.GetTokens(ctx, []int64{job.ExecutionToken})
	if err != nil {
		return res, fmt.Errorf("failed to find job token %d: %w", job.ExecutionToken, err)
	}
	if len(tokens) != 1 {
		return res, fmt.Errorf("failed to find job token %d in the database", job.ExecutionToken)
	}
	token := tokens[0]

	var variables map[string]interface{}
	err = json.Unmarshal([]byte(job.Variables), &variables)
	if err != nil {
		return res, fmt.Errorf("failed to unmarshal job variables: %w", err)
	}
	res = bpmnruntime.Job{
		ElementId:          job.ElementID,
		ElementInstanceKey: job.ElementInstanceKey,
		ProcessInstanceKey: job.ProcessInstanceKey,
		Key:                job.Key,
		Type:               job.Type,
		State:              bpmnruntime.ActivityState(job.State),
		CreatedAt:          time.UnixMilli(job.CreatedAt),
		Token: bpmnruntime.ExecutionToken{
			Key:                token.Key,
			ElementInstanceKey: token.ElementInstanceKey,
			ElementId:          token.ElementID,
			ProcessInstanceKey: token.ProcessInstanceKey,
			State:              bpmnruntime.TokenState(token.State),
		},
		Variables: variables,
	}
	return res, nil
}

func (rq *DB) FindPendingProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.Job, error) {
	dbJobs, err := rq.Queries.FindProcessInstanceJobsInState(ctx, sql.FindProcessInstanceJobsInStateParams{
		ProcessInstanceKey: processInstanceKey,
		States:             []int64{int64(bpmnruntime.ActivityStateCompleting), int64(bpmnruntime.ActivityStateActive), int64(bpmnruntime.ActivityStateFailed)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find pending process instance jobs for process instance key %d: %w", processInstanceKey, err)
	}
	res := make([]bpmnruntime.Job, len(dbJobs))
	tokensToLoad := make([]int64, len(dbJobs))
	for i, job := range dbJobs {
		var variables map[string]interface{}
		err = json.Unmarshal([]byte(job.Variables), &variables)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal job variables: %w", err)
		}
		res[i] = bpmnruntime.Job{
			ElementId:          job.ElementID,
			ElementInstanceKey: job.ElementInstanceKey,
			ProcessInstanceKey: job.ProcessInstanceKey,
			Key:                job.Key,
			Type:               job.Type,
			State:              bpmnruntime.ActivityState(job.State),
			CreatedAt:          time.UnixMilli(job.CreatedAt),
			Token: bpmnruntime.ExecutionToken{
				Key: job.ExecutionToken,
			},
			Variables: variables,
		}
		tokensToLoad[i] = job.ExecutionToken
	}
	loadedTokens, err := rq.Queries.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return nil, fmt.Errorf("failed to load message subscriptions tokens: %w", err)
	}
	for _, token := range loadedTokens {
		// we might have the same token registered for multiple subs (event base gateway) so we have to go through whole array
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = bpmnruntime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              bpmnruntime.TokenState(token.State),
				}
			}
		}
	}
	return res, nil
}

var _ storage.JobStorageWriter = &DB{}

func (rq *DB) SaveJob(ctx context.Context, job bpmnruntime.Job) error {
	return SaveJobWith(ctx, rq.Queries, job)
}

func SaveJobWith(ctx context.Context, db *sql.Queries, job bpmnruntime.Job) error {
	variableBytes, err := json.Marshal(job.Variables)
	if err != nil {
		return fmt.Errorf("failed to marshal variables for job %d: %w", job.GetKey(), err)
	}
	err = db.SaveJob(ctx, sql.SaveJobParams{
		Key:                job.GetKey(),
		ElementID:          job.ElementId,
		ElementInstanceKey: job.ElementInstanceKey,
		ProcessInstanceKey: job.ProcessInstanceKey,
		Type:               job.Type,
		State:              int64(job.GetState()),
		CreatedAt:          job.CreatedAt.UnixMilli(),
		Variables:          string(variableBytes),
		ExecutionToken:     job.Token.Key,
	})
	if err != nil {
		return fmt.Errorf("failed to save job %d: %w", job.GetKey(), err)
	}
	return nil
}

func (rq *DB) SaveMessageSubscriptionPointer(ctx context.Context, pointer sql.MessageSubscriptionPointer) error {
	zenState := rq.zenState()
	ptrPartitionId := zenState.GetPartitionIdFromString(pointer.CorrelationKey)
	upsertPointer := func() error {
		err := rq.Queries.SaveMessageSubscriptionPointer(ctx, sql.SaveMessageSubscriptionPointerParams{
			State:                  int64(pointer.State),
			CreatedAt:              pointer.CreatedAt,
			Name:                   pointer.Name,
			CorrelationKey:         pointer.CorrelationKey,
			MessageSubscriptionKey: pointer.MessageSubscriptionKey,
			ExecutionTokenKey:      pointer.ExecutionTokenKey,
		})
		if err != nil {
			return fmt.Errorf("failed to save message subscription pointer for message subscription %d: %w", pointer.MessageSubscriptionKey, err)
		}
		return nil
	}

	if rq.Partition == ptrPartitionId && rq.Store.IsLeader() {
		oldPointer, err := rq.Queries.FindMessageSubscriptionPointer(ctx, sql.FindMessageSubscriptionPointerParams{
			CorrelationKey: pointer.CorrelationKey,
			Name:           pointer.Name,
			FilterState:    int64(bpmnruntime.ActivityStateActive),
		})
		// if pointer does not exist create it
		if errors.Is(err, sql.ErrNoRows) {
			return upsertPointer()
		}

		// check if message is active in partition
		msgPartitionId := zenflake.GetPartitionId(oldPointer.MessageSubscriptionKey)
		messageLeader, err := rq.client.PartitionLeader(msgPartitionId)
		if err != nil {
			return fmt.Errorf("failed to get partition %d leader client: %w", msgPartitionId, err)
		}
		foundMessage, err := messageLeader.FindActiveMessage(ctx, &zenproto.FindActiveMessageRequest{
			Name:              &oldPointer.Name,
			CorrelationKey:    &oldPointer.CorrelationKey,
			ExecutionTokenKey: &oldPointer.ExecutionTokenKey,
		})
		if grpccode.NotFound == status.Code(err) {
			// if message is not active
			return upsertPointer()
		} else if err != nil {
			return fmt.Errorf("failed to check if message assigned to pointer is active: %w", err)
		}

		// check if pointer exists
		// if yes check if message exists in its partition
		//   if the message is active forbid creation of new ptr
		//   if the message is completed overwrite ptr
		return fmt.Errorf("message %d has already active subscription for this correlationKey", foundMessage.Key)
	}

	leader, err := rq.client.PartitionLeader(ptrPartitionId)
	if err != nil {
		return fmt.Errorf("failed to get client for partition leader %d: %w", ptrPartitionId, err)
	}
	resp, err := leader.SetMessageSubscriptionPointer(ctx, &zenproto.SetMessageSubscriptionPointerRequest{
		State:                  &pointer.State,
		CreatedAt:              &pointer.CreatedAt,
		Name:                   &pointer.Name,
		CorrelationKey:         &pointer.CorrelationKey,
		MessageSubscriptionKey: &pointer.MessageSubscriptionKey,
		ExecutionTokenKey:      &pointer.ExecutionTokenKey,
		PartitionId:            &ptrPartitionId,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to save message subscription pointer for message subscription %d: %w", pointer.MessageSubscriptionKey, err)
		if err != nil {
			return fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}
	return nil
}

func (rq *DB) FindActiveMessageSubscriptionPointer(ctx context.Context, name string, correlationKey string) (sql.MessageSubscriptionPointer, error) {
	dbMessageSub, err := rq.Queries.FindMessageSubscriptionPointer(ctx, sql.FindMessageSubscriptionPointerParams{
		CorrelationKey: correlationKey,
		Name:           name,
		FilterState:    int64(bpmnruntime.ActivityStateActive),
	})
	if err != nil {
		return dbMessageSub, fmt.Errorf("failed to find ready message subscription pointer for name %s correlationKey %s: %w", name, correlationKey, err)
	}

	return dbMessageSub, nil
}

func (rq *DB) FindActiveMessageSubscriptionKey(ctx context.Context, name string, correlationKey string) (int64, error) {
	dbMessageSub, err := rq.Queries.FindMessageSubscriptionByNameAndCorrelationKeyAndState(ctx, sql.FindMessageSubscriptionByNameAndCorrelationKeyAndStateParams{
		Name:           name,
		CorrelationKey: correlationKey,
		State:          int64(bpmnruntime.ActivityStateActive),
	})
	if err != nil {
		return 0, err
	}
	return dbMessageSub.Key, nil
}

var _ storage.MessageStorageReader = &DB{}

func (rq *DB) FindTokenMessageSubscriptions(ctx context.Context, tokenKey int64, state bpmnruntime.ActivityState) ([]bpmnruntime.MessageSubscription, error) {

	dbMessages, err := rq.Queries.FindTokenMessageSubscriptions(ctx, sql.FindTokenMessageSubscriptionsParams{
		ExecutionToken: tokenKey,
		State:          int64(state),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find token message subscriptions for token %d: %w", tokenKey, err)
	}
	res := make([]bpmnruntime.MessageSubscription, len(dbMessages))

	loadedTokens, err := rq.Queries.GetTokens(ctx, []int64{tokenKey})
	if err != nil || len(loadedTokens) != 1 {
		return res, fmt.Errorf("failed to load message subscription token: %w", err)
	}

	token := bpmnruntime.ExecutionToken{
		Key:                loadedTokens[0].Key,
		ElementInstanceKey: loadedTokens[0].ElementInstanceKey,
		ElementId:          loadedTokens[0].ElementID,
		ProcessInstanceKey: loadedTokens[0].ProcessInstanceKey,
		State:              bpmnruntime.TokenState(loadedTokens[0].State),
	}

	for i, mes := range dbMessages {
		res[i] = bpmnruntime.MessageSubscription{
			Key:                  mes.Key,
			ElementId:            mes.ElementID,
			ElementInstanceKey:   mes.ElementInstanceKey,
			ProcessDefinitionKey: mes.ProcessDefinitionKey,
			ProcessInstanceKey:   mes.ProcessInstanceKey,
			Name:                 mes.Name,
			CorrelationKey:       mes.CorrelationKey,
			State:                bpmnruntime.ActivityState(mes.State),
			CreatedAt:            time.UnixMilli(mes.CreatedAt),
			Token:                token,
		}
	}
	return res, nil
}

// TODO rename Id to ElementInstanceKey
func (rq *DB) FindMessageSubscriptionById(ctx context.Context, messageSubscriptionKey int64, state bpmnruntime.ActivityState) (bpmnruntime.MessageSubscription, error) {
	var res bpmnruntime.MessageSubscription
	dbMessage, err := rq.Queries.GetMessageSubscriptionById(ctx, sql.GetMessageSubscriptionByIdParams{
		Key:   messageSubscriptionKey,
		State: int64(state),
	})
	if err != nil {
		return res, fmt.Errorf("failed to find active message subscription %d: %w", messageSubscriptionKey, err)
	}
	res = bpmnruntime.MessageSubscription{
		Key:                  dbMessage.Key,
		ElementId:            dbMessage.ElementID,
		ElementInstanceKey:   dbMessage.ElementInstanceKey,
		ProcessDefinitionKey: dbMessage.ProcessDefinitionKey,
		ProcessInstanceKey:   dbMessage.ProcessInstanceKey,
		Name:                 dbMessage.Name,
		CorrelationKey:       dbMessage.CorrelationKey,
		State:                bpmnruntime.ActivityState(dbMessage.State),
		CreatedAt:            time.UnixMilli(dbMessage.CreatedAt),
		Token: bpmnruntime.ExecutionToken{
			Key: dbMessage.ExecutionToken,
		},
	}

	loadedTokens, err := rq.Queries.GetTokens(ctx, []int64{dbMessage.ExecutionToken})
	if err != nil || len(loadedTokens) != 1 {
		return res, fmt.Errorf("failed to load message subscription token: %w", err)
	}

	res.Token = bpmnruntime.ExecutionToken{
		Key:                loadedTokens[0].Key,
		ElementInstanceKey: loadedTokens[0].ElementInstanceKey,
		ElementId:          loadedTokens[0].ElementID,
		ProcessInstanceKey: loadedTokens[0].ProcessInstanceKey,
		State:              bpmnruntime.TokenState(loadedTokens[0].State),
	}

	return res, nil
}

func (rq *DB) FindProcessInstanceMessageSubscriptions(ctx context.Context, processInstanceKey int64, state bpmnruntime.ActivityState) ([]bpmnruntime.MessageSubscription, error) {
	dbMessages, err := rq.Queries.FindProcessInstanceMessageSubscriptions(ctx, sql.FindProcessInstanceMessageSubscriptionsParams{
		ProcessInstanceKey: processInstanceKey,
		State:              int64(state),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find message subscriptions for process %d: %w", processInstanceKey, err)
	}
	res := make([]bpmnruntime.MessageSubscription, len(dbMessages))
	tokensToLoad := make([]int64, len(dbMessages))
	for i, mes := range dbMessages {
		res[i] = bpmnruntime.MessageSubscription{
			Key:                  mes.Key,
			ElementId:            mes.ElementID,
			ElementInstanceKey:   mes.ElementInstanceKey,
			ProcessDefinitionKey: mes.ProcessDefinitionKey,
			ProcessInstanceKey:   mes.ProcessInstanceKey,
			Name:                 mes.Name,
			CorrelationKey:       mes.CorrelationKey,
			State:                bpmnruntime.ActivityState(mes.State),
			CreatedAt:            time.UnixMilli(mes.CreatedAt),
			Token: bpmnruntime.ExecutionToken{
				Key: mes.ExecutionToken,
			},
		}
		tokensToLoad[i] = mes.ExecutionToken
	}
	loadedTokens, err := rq.Queries.GetTokens(ctx, tokensToLoad)
	if err != nil {
		return nil, fmt.Errorf("failed to load message subscriptions tokens: %w", err)
	}
	for _, token := range loadedTokens {
		// we might have the same token registered for multiple subs (event base gateway) so we have to go through whole array
		for i := range res {
			if res[i].Token.Key == token.Key {
				res[i].Token = bpmnruntime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              bpmnruntime.TokenState(token.State),
				}
			}
		}
	}
	return res, nil
}

var _ storage.MessageStorageWriter = &DB{}

func (rq *DB) SaveMessageSubscription(ctx context.Context, subscription bpmnruntime.MessageSubscription) error {
	err := rq.SaveMessageSubscriptionPointer(ctx, sql.MessageSubscriptionPointer{
		State:                  int64(subscription.State),
		CreatedAt:              subscription.CreatedAt.UnixMilli(),
		Name:                   subscription.Name,
		CorrelationKey:         subscription.CorrelationKey,
		MessageSubscriptionKey: subscription.Key,
		ExecutionTokenKey:      subscription.Token.Key,
	})
	if err != nil {
		return fmt.Errorf("failed to update message pointer: %w", err)
	}
	return SaveMessageSubscriptionWith(ctx, rq.Queries, subscription)
}

func SaveMessageSubscriptionWith(ctx context.Context, db *sql.Queries, subscription bpmnruntime.MessageSubscription) error {
	fmt.Printf("save message sub %+v", subscription)
	err := db.SaveMessageSubscription(ctx, sql.SaveMessageSubscriptionParams{
		Key:                  subscription.GetKey(),
		ElementID:            subscription.ElementId,
		ElementInstanceKey:   subscription.ElementInstanceKey,
		ProcessDefinitionKey: subscription.ProcessDefinitionKey,
		ProcessInstanceKey:   subscription.ProcessInstanceKey,
		Name:                 subscription.Name,
		State:                int64(subscription.GetState()),
		CreatedAt:            subscription.CreatedAt.UnixMilli(),
		CorrelationKey:       subscription.CorrelationKey,
		ExecutionToken:       subscription.Token.Key,
	})
	if err != nil {
		return fmt.Errorf("failed to save message subscription %d: %w", subscription.GetKey(), err)
	}
	return nil
}

var _ storage.TokenStorageReader = &DB{}

func (rq *DB) GetRunningTokens(ctx context.Context) ([]bpmnruntime.ExecutionToken, error) {
	return GetActiveTokens(ctx, rq.Queries, rq.Partition)
}

func GetActiveTokens(ctx context.Context, db *sql.Queries, partitionId uint32) ([]bpmnruntime.ExecutionToken, error) {
	tokens, err := db.GetTokensInState(ctx, int64(bpmnruntime.TokenStateRunning))
	res := make([]bpmnruntime.ExecutionToken, len(tokens))
	for i, tok := range tokens {
		res[i] = bpmnruntime.ExecutionToken{
			Key:                tok.Key,
			ElementInstanceKey: tok.ElementInstanceKey,
			ElementId:          tok.ElementID,
			ProcessInstanceKey: tok.ProcessInstanceKey,
			State:              bpmnruntime.TokenState(tok.State),
		}
	}
	return res, err
}

func (rq *DB) GetTokensForProcessInstance(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error) {
	return GetTokensForProcessInstance(ctx, rq.Queries, rq.Partition, processInstanceKey)
}

func GetTokensForProcessInstance(ctx context.Context, db *sql.Queries, partitionId uint32, processInstanceKey int64) ([]bpmnruntime.ExecutionToken, error) {
	tokens, err := db.GetTokensForProcessInstance(ctx, processInstanceKey)
	res := make([]bpmnruntime.ExecutionToken, len(tokens))
	for i, tok := range tokens {
		res[i] = bpmnruntime.ExecutionToken{
			Key:                tok.Key,
			ElementInstanceKey: tok.ElementInstanceKey,
			ElementId:          tok.ElementID,
			ProcessInstanceKey: tok.ProcessInstanceKey,
			State:              bpmnruntime.TokenState(tok.State),
		}
	}
	return res, err
}

var _ storage.TokenStorageWriter = &DB{}

func (rq *DB) SaveToken(ctx context.Context, token bpmnruntime.ExecutionToken) error {
	return SaveToken(ctx, rq.Queries, token)
}

func SaveToken(ctx context.Context, db *sql.Queries, token bpmnruntime.ExecutionToken) error {
	return db.SaveToken(ctx, sql.SaveTokenParams{
		Key:                token.Key,
		ElementInstanceKey: token.ElementInstanceKey,
		ElementID:          token.ElementId,
		ProcessInstanceKey: token.ProcessInstanceKey,
		State:              int64(token.State),
		CreatedAt:          time.Now().UnixMilli(),
	})
}

func (rq *DB) SaveFlowElementHistory(ctx context.Context, historyItem bpmnruntime.FlowElementHistoryItem) error {
	return SaveFlowElementHistoryWith(ctx, rq.Queries, historyItem)
}

func SaveFlowElementHistoryWith(ctx context.Context, db *sql.Queries, historyItem bpmnruntime.FlowElementHistoryItem) error {
	return db.SaveFlowElementHistory(
		ctx,
		sql.SaveFlowElementHistoryParams{
			ElementInstanceKey: historyItem.ElementInstanceKey,
			ElementID:          historyItem.ElementId,
			ProcessInstanceKey: historyItem.ProcessInstanceKey,
			CreatedAt:          historyItem.CreatedAt.UnixMilli(),
		},
	)
}

var _ storage.IncidentStorageReader = &DB{}

func (rq *DB) FindIncidentByKey(ctx context.Context, key int64) (bpmnruntime.Incident, error) {
	return FindIncidentByKey(ctx, rq.Queries, key)
}

func FindIncidentByKey(ctx context.Context, db *sql.Queries, key int64) (bpmnruntime.Incident, error) {
	incident, err := db.FindIncidentByKey(ctx, key)
	if err != nil {
		return bpmnruntime.Incident{}, err
	}

	tokens, err := db.GetTokens(ctx, []int64{incident.ExecutionToken})
	if len(tokens) == 0 {
		err = errors.Join(err, errors.New("no incidents found"))
	}
	token := tokens[0]
	return bpmnruntime.Incident{
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
		Token: bpmnruntime.ExecutionToken{
			Key:                token.Key,
			ElementInstanceKey: token.ElementInstanceKey,
			ElementId:          token.ElementID,
			ProcessInstanceKey: token.ProcessInstanceKey,
			State:              bpmnruntime.TokenState(token.State),
		},
	}, nil
}

func (rq *DB) FindIncidentsByProcessInstanceKey(ctx context.Context, processInstanceKey int64) ([]bpmnruntime.Incident, error) {
	return FindIncidentsByProcessInstanceKey(ctx, rq.Queries, processInstanceKey)
}

func FindIncidentsByProcessInstanceKey(ctx context.Context, db *sql.Queries, processInstanceKey int64) ([]bpmnruntime.Incident, error) {
	incidents, err := db.FindIncidentsByProcessInstanceKey(ctx, processInstanceKey)
	if err != nil {
		return nil, err
	}
	res := make([]bpmnruntime.Incident, len(incidents))

	tokensToLoad := make([]int64, len(incidents))
	for i, incident := range incidents {
		res[i] = bpmnruntime.Incident{
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
				res[i].Token = bpmnruntime.ExecutionToken{
					Key:                token.Key,
					ElementInstanceKey: token.ElementInstanceKey,
					ElementId:          token.ElementID,
					ProcessInstanceKey: token.ProcessInstanceKey,
					State:              bpmnruntime.TokenState(token.State),
				}
				continue token
			}
		}
	}
	return res, nil
}

var _ storage.IncidentStorageWriter = &DB{}

func (rq *DB) SaveIncident(ctx context.Context, incident bpmnruntime.Incident) error {
	return SaveIncidentWith(ctx, rq.Queries, incident)
}

func SaveIncidentWith(ctx context.Context, db *sql.Queries, incident bpmnruntime.Incident) error {
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

type DBBatch struct {
	db               *DB
	stmtToRun        []*proto.Statement
	queries          *sql.Queries
	postFlushActions []func()
	preFlushActions  []func() error
	logger           hclog.Logger
}

func (d *DBBatch) getQueries() *sql.Queries {
	return d.queries
}

func (d *DBBatch) getReadDB() *DB {
	return d.db
}

func (d *DBBatch) getLogger() hclog.Logger {
	return d.logger
}

func (rq *DBBatch) ExecContext(ctx context.Context, sql string, args ...interface{}) (ssql.Result, error) {
	stmt := rq.db.generateStatement(sql, args...)
	rq.stmtToRun = append(rq.stmtToRun, stmt)
	return rqliteResult{}, nil
}

func (rq *DBBatch) PrepareContext(ctx context.Context, sql string) (*ssql.Stmt, error) {
	panic("PrepareContext not supported by RqLiteDBBatch")
}

func (rq *DBBatch) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	panic("QueryContext not supported by RqLiteDBBatch")
}

func (rq *DBBatch) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	panic("QueryRowContext not supported by RqLiteDBBatch")
}

var _ storage.Batch = &DBBatch{}

func (b *DBBatch) AddPostFlushAction(ctx context.Context, f func()) {
	b.postFlushActions = append(b.postFlushActions, f)
}
func (b *DBBatch) AddPreFlushAction(ctx context.Context, f func() error) {
	b.preFlushActions = append(b.preFlushActions, f)
}

func (b *DBBatch) Flush(ctx context.Context) error {
	for _, action := range b.preFlushActions {
		err := action()
		if err != nil {
			funcName := runtime.FuncForPC(reflect.ValueOf(action).Pointer()).Name()
			return fmt.Errorf("failed pre-flush action %s: %w", funcName, err)
		}
	}
	ctx, execSpan := b.db.tracer.Start(ctx, "rqlite-batch", trace.WithAttributes(
		attribute.String(otelPkg.AttributeExec, fmt.Sprintf("%v", b.stmtToRun)),
	))
	defer func() {
		execSpan.End()
	}()
	resp, err := b.db.ExecuteStatements(ctx, b.stmtToRun)
	_ = resp
	if err != nil {
		b.logger.Error(fmt.Sprintf("failed to flush statements: %s", err))
		return err
	}
	var stmtErr error
	for i, res := range resp {
		if res.GetError() != "" {
			stmtErr = errors.Join(fmt.Errorf("statement %d error: %s", i, res.GetError()))
		}
	}
	if stmtErr != nil {
		b.logger.Error(fmt.Sprintf("failed to flush - statements error: %s", stmtErr))
		return stmtErr
	}
	for _, action := range b.postFlushActions {
		action()
	}
	return nil
}

var _ storage.ProcessDefinitionStorageWriter = &DBBatch{}

func (b *DBBatch) SaveProcessDefinition(ctx context.Context, definition bpmnruntime.ProcessDefinition) error {
	return SaveProcessDefinitionWith(ctx, b.queries, definition)
}

var _ storage.ProcessInstanceStorageWriter = &DBBatch{}

func (b *DBBatch) SaveProcessInstance(ctx context.Context, processInstance bpmnruntime.ProcessInstance) error {
	return SaveProcessInstanceWith(ctx, b, processInstance)
}

var _ storage.TimerStorageWriter = &DBBatch{}

func (b *DBBatch) SaveTimer(ctx context.Context, timer bpmnruntime.Timer) error {
	return SaveTimerWith(ctx, b.queries, timer)
}

var _ storage.JobStorageWriter = &DBBatch{}

func (b *DBBatch) SaveJob(ctx context.Context, job bpmnruntime.Job) error {
	return SaveJobWith(ctx, b.queries, job)
}

var _ storage.MessageStorageWriter = &DBBatch{}

func (b *DBBatch) SaveMessageSubscription(ctx context.Context, subscription bpmnruntime.MessageSubscription) error {
	b.AddPreFlushAction(ctx, func() error {
		err := b.db.SaveMessageSubscriptionPointer(ctx, sql.MessageSubscriptionPointer{
			State:                  int64(subscription.State),
			CreatedAt:              subscription.CreatedAt.UnixMilli(),
			Name:                   subscription.Name,
			CorrelationKey:         subscription.CorrelationKey,
			MessageSubscriptionKey: subscription.Key,
			ExecutionTokenKey:      subscription.Token.Key,
		})
		if err != nil {
			return fmt.Errorf("failed to update message %d pointer: %w", subscription.Key, err)
		}
		return nil
	})
	return SaveMessageSubscriptionWith(ctx, b.queries, subscription)
}

var _ storage.TokenStorageWriter = &DBBatch{}

func (b *DBBatch) SaveToken(ctx context.Context, token bpmnruntime.ExecutionToken) error {
	return SaveToken(ctx, b.queries, token)
}

func (b *DBBatch) SaveFlowElementHistory(ctx context.Context, historyItem bpmnruntime.FlowElementHistoryItem) error {
	return SaveFlowElementHistoryWith(ctx, b.queries, historyItem)
}

var _ storage.IncidentStorageWriter = &DBBatch{}

func (b *DBBatch) SaveIncident(ctx context.Context, incident bpmnruntime.Incident) error {
	return SaveIncidentWith(ctx, b.queries, incident)
}
