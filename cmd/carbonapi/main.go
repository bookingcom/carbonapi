package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/bookingcom/carbonapi/pkg/app/carbonapi"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"go.uber.org/zap"
)

// BuildVersion is provided to be overridden at build time. Eg. go build -ldflags -X 'main.BuildVersion=...'
var BuildVersion = "(development build)"

func main() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "PANIC: %v\n", r)
		}
	}()

	configPath := flag.String("config", "", "Path to the `config file`.")
	flag.Parse()

	fh, err := os.Open(*configPath)
	if err != nil {
		log.Fatalf("Failed to open config file: %s", err)
	}

	apiConfig, err := cfg.ParseAPIConfig(fh)
	if err != nil {
		log.Fatalf("Failed to parse config file: %f", err)
	}
	fh.Close()

	if apiConfig.MaxProcs != 0 {
		runtime.GOMAXPROCS(apiConfig.MaxProcs)
	}
	logger, err := apiConfig.LoggerConfig.Build()
	if err != nil {
		log.Fatalf("Failed to initiate logger: %s", err)
	}
	logger = logger.Named("carbonapi")
	defer func() {
		if syncErr := logger.Sync(); syncErr != nil {
			log.Fatalf("could not sync the logger: %s", syncErr)
		}
	}()

	log.Printf("starting carbonapi - build_version: %s, apiConfig: %+v", BuildVersion, apiConfig)
	logger.Info("starting carbonapi",
		zap.String("build_version", BuildVersion),
		zap.String("apiConfig", fmt.Sprintf("%+v", apiConfig)),
	)

	app, err := carbonapi.New(apiConfig, logger, BuildVersion)
	if err != nil {
		logger.Error("Error initializing app")
	}
	flush := app.Start(logger)
	defer flush()
}
