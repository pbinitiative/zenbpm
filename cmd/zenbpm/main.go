package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/profile"
	"github.com/pbinitiative/zenbpm/internal/rest"
	bpmn_engine "github.com/pbinitiative/zenbpm/pkg/bpmn"
)

func main() {
	profile.InitProfile()
	log.Init()

	appContext, ctxCancel := context.WithCancel(context.Background())

	conf := config.InitConfig()

	// TODO: initialize cluster client

	zenNode, err := cluster.StartZenNode(appContext, conf)
	if err != nil {
		log.Error("Failed to start Zen node: %s", err)
		os.Exit(1)
	}

	// TODO: engine and persistence start should be moved into the controller/partition
	var store storage.PersistentStorage = zenNode
	time.Sleep(2 * time.Second)
	engine := bpmn_engine.New(bpmn_engine.WithStorage(store), bpmn_engine.WithRqliteExporter())
	// TODO rework handlers
	emptyHandler := func(job bpmn_engine.ActivatedJob) {
	}
	engine.NewTaskHandler().Type("foo").Handler(emptyHandler)

	// Start the public API
	svr := rest.NewServer(&engine, conf.Server.Addr)
	svr.Start()

	appStop := make(chan os.Signal, 2)
	signal.Notify(appStop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	handleSigterm(appStop, appContext)

	ctxCancel()
	// cleanup
	svr.Stop(appContext)
	err = zenNode.Stop()
	if err != nil {
		log.Error("failed to properly stop zen node: %s", err)
	}
}

func handleSigterm(appStop chan os.Signal, ctx context.Context) {
	signal.Notify(appStop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	sig := <-appStop
	log.Infof(ctx, "Received %s. Shutting down", sig.String())
}
