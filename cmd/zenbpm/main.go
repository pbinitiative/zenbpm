package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pbinitiative/zenbpm/internal/buildinfo"
	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/errortracking"
	"github.com/pbinitiative/zenbpm/internal/grpc"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/otel"

	"github.com/pbinitiative/zenbpm/internal/profile"
	"github.com/pbinitiative/zenbpm/internal/rest"
)

func main() {
	os.Exit(run())
}

func run() int {
	profile.InitProfile()
	log.Init()
	buildInfo := buildinfo.Current()
	log.Info("Starting ZenBPM version %s", buildInfo.Version)

	if err := errortracking.Init(buildInfo.Version, string(profile.Current)); err != nil {
		log.Error("GlitchTip error tracking is disabled because initialization failed: %s", err)
	}
	defer func() {
		if !errortracking.Flush(2 * time.Second) {
			log.Warn("Timed out while flushing GlitchTip events")
		}
	}()

	appContext, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	conf := config.InitConfig()

	openTelemetry, err := otel.SetupOtel(conf.Tracing)
	if err != nil {
		log.Error("Failed to set up OTEL: %s", err)
		return 1
	}

	// TODO: initialize cluster client
	zenNode, err := cluster.StartZenNode(appContext, conf)
	if err != nil {
		log.Error("Failed to start Zen node: %s", err)
		return 1
	}

	// Start the public API
	svr := rest.NewServer(zenNode, conf, buildInfo)
	svr.Start()

	// Start ZenBpm GRPC API
	grpcSrv := grpc.NewServer(appContext, zenNode, conf.GrpcServer.Addr)
	grpcSrv.Start()

	appStop := make(chan os.Signal, 2)
	signal.Notify(appStop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	handleSigterm(appStop, appContext)

	ctxCancel()
	// cleanup
	svr.Stop(appContext)
	grpcSrv.Stop()
	err = zenNode.Stop()
	if err != nil {
		log.Error("failed to properly stop zen node: %s", err)
	}
	openTelemetry.Stop(appContext)
	return 0
}

func handleSigterm(appStop chan os.Signal, ctx context.Context) {
	signal.Notify(appStop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	sig := <-appStop
	log.Infof(ctx, "Received %s. Shutting down", sig.String())
}
