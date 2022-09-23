package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/bookingcom/carbonapi/pkg/app/zipper"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"github.com/bookingcom/carbonapi/pkg/trace"
	"github.com/dgryski/go-expirecache"
	"github.com/facebookgo/pidfile"
	"go.uber.org/zap"
)

var BuildVersion string

func main() {
	configFile := flag.String("config", "", "config file (yaml)")
	flag.Parse()

	err := pidfile.Write()
	if err != nil && !pidfile.IsNotConfigured(err) {
		log.Fatalln("error during pidfile.Write():", err)
	}

	if *configFile == "" {
		log.Fatal("missing config file option")
	}

	fh, err := os.Open(*configFile)
	if err != nil {
		log.Fatalf("unable to read config file: %s", err)
	}

	config, err := cfg.ParseZipperConfig(fh)
	if err != nil {
		log.Fatalf("failed to parse config at %s: %s", *configFile, err)
	}
	fh.Close()

	if config.MaxProcs != 0 {
		runtime.GOMAXPROCS(config.MaxProcs)
	}

	if len(config.GetBackends()) == 0 {
		log.Fatal("no Backends loaded -- exiting")
	}

	logger, err := config.LoggerConfig.Build()
	if err != nil {
		log.Fatalf("failed to initiate logger: %s", err)
	}
	logger = logger.Named("carbonzipper")
	defer func() {
		if syncErr := logger.Sync(); syncErr != nil {
			log.Fatalf("could not sync the logger: %v", syncErr)
		}
	}()

	logger.Info("starting carbonzipper",
		zap.String("build_version", BuildVersion),
		zap.String("zipperConfig", fmt.Sprintf("%+v", config)),
	)

	ms := zipper.NewPrometheusMetrics(config)
	bs, err := zipper.InitBackends(config, ms, logger)
	if err != nil {
		logger.Fatal("failed to init backends", zap.Error(err))
	}

	app := &zipper.App{
		Config:              config,
		Metrics:             ms,
		Backends:            bs,
		TopLevelDomainCache: expirecache.New(0),
		TLDPrefixes:         zipper.InitTLDPrefixes(logger, config.TLDCacheExtraPrefixes),
	}

	flush := trace.InitTracer(BuildVersion, "carbonzipper", logger, app.Config.Traces)
	defer flush()

	app.Start(logger)
}
