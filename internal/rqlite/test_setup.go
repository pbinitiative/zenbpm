package rqlite

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/bwmarrin/snowflake"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/db"

	"github.com/rqlite/rqlite/v8/random"
)

type testDatabase struct {
	db *db.SwappableDB
}

func (s *testDatabase) Query(ctx context.Context, req *proto.QueryRequest) ([]*proto.QueryRows, error) {
	return s.db.Query(req.Request, false)
}

func (s *testDatabase) Execute(ctx context.Context, req *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error) {
	return s.db.Execute(req.Request, false)
}

func (s *testDatabase) IsLeader(ctx context.Context) bool {
	return true
}

type TestStorage struct {
	gen               *snowflake.Node
	rqlitePersistence *PersistenceRqlite
	Store             storage.PersistentStorage
	testDirPath       string
}

func (s *TestStorage) SetupTestEnvironment(m *testing.M) {
	g, err := snowflake.NewNode(1)
	if err != nil {
		log.Errorf(context.TODO(), "Error while initing snowflake: %s", err.Error())
	}
	s.gen = g

	wd, _ := os.Getwd()
	s.testDirPath = path.Join(wd, random.String())
	// dbConf := store.NewDBConfig("test", true)
	// dbConf.FKConstraints = true

	dbPath := path.Join(s.testDirPath, "db.sqlite")
	err = os.MkdirAll(s.testDirPath, 0770)
	if err != nil {
		fmt.Printf("failed to test directory: %s", err)
		os.Exit(1)
	}
	f, err := os.Create(dbPath)
	if err != nil {
		fmt.Printf("failed to create database file: %s", err)
		os.Exit(1)
	}
	f.Close()
	d, err := db.OpenSwappable(dbPath, true, false)
	if err != nil {
		fmt.Printf("failed to open database: %s\n", err)
		os.Exit(1)
	}
	db := testDatabase{db: d}

	s.Store = &db
	s.rqlitePersistence = NewPersistenceRqlite(
		s.Store,
	)
}

func (s *TestStorage) TeardownTestEnvironment(m *testing.M) {
	os.RemoveAll(s.testDirPath)

}
