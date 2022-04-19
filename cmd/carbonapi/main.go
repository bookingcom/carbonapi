package main

import (
	"expvar"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/bookingcom/carbonapi/app/carbonapi"
	"github.com/bookingcom/carbonapi/cfg"
	"github.com/lomik/zapwriter"
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

	err := zapwriter.ApplyConfig([]zapwriter.Config{cfg.GetDefaultLoggerConfig()})
	if err != nil {
		log.Fatal("Failed to initialize logger with default configuration")
	}
	logger := zapwriter.Logger("main")

	configPath := flag.String("config", "", "Path to the `config file`.")
	flag.Parse()

	fh, err := os.Open(*configPath)
	if err != nil {
		logger.Fatal("Failed to open config file",
			zap.Error(err),
		)
	}

	apiConfig, err := cfg.ParseAPIConfig(fh)
	if err != nil {
		logger.Fatal("Failed to parse config file",
			zap.Error(err),
		)
	}
	fh.Close()

	if configErr := zapwriter.ApplyConfig(apiConfig.Logger); configErr != nil {
		logger.Fatal("Failed to apply config",
			zap.Any("config", apiConfig.Logger),
			zap.Error(configErr),
		)
	}
	if apiConfig.MaxProcs != 0 {
		runtime.GOMAXPROCS(apiConfig.MaxProcs)
	}
	expvar.NewString("BuildVersion").Set(BuildVersion)
	logger.Info("starting carbonapi",
		zap.String("build_version", BuildVersion),
		zap.Any("apiConfig", apiConfig),
	)
	app, err := carbonapi.New(apiConfig, logger, BuildVersion)
	if err != nil {
		logger.Error("Error initializing app")
	}
	appLogger := zapwriter.Logger("carbonapi")
	flush := app.Start(appLogger)
	defer flush()
}
