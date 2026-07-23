package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/grpc"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/otel"

	"github.com/pbinitiative/zenbpm/internal/profile"
	"github.com/pbinitiative/zenbpm/internal/rest"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

var commit = "unknown"

func main() {
	profile.InitProfile()
	log.Init()

	description, err := buildDescription(commit)
	if err != nil {
		log.Error("Failed to load embedded OpenAPI specification: %s", err)
		os.Exit(1)
	}
	log.Info("Starting ZenBPM %s", description)

	appContext, ctxCancel := context.WithCancel(context.Background())

	conf := config.InitConfig()

	openTelemetry, err := otel.SetupOtel(conf.Tracing)
	if err != nil {
		log.Error("Failed to set up OTEL: %s", err)
		os.Exit(1)
	}

	// TODO: initialize cluster client
	zenNode, err := cluster.StartZenNode(appContext, conf)
	if err != nil {
		log.Error("Failed to start Zen node: %s", err)
		os.Exit(1)
	}

	// Start the public API
	svr := rest.NewServer(zenNode, conf, commit)
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
}

func buildDescription(commit string) (string, error) {
	api, err := public.GetSwagger()
	if err != nil {
		return "", err
	}
	return "v" + api.Info.Version + " (" + commit + ")", nil
}

func handleSigterm(appStop chan os.Signal, ctx context.Context) {
	signal.Notify(appStop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	sig := <-appStop
	log.Infof(ctx, "Received %s. Shutting down", sig.String())
}
