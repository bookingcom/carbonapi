package main

import (
	"expvar"
	"flag"
	"log"
	"os"
	"runtime"

	zipper "github.com/bookingcom/carbonapi/app/carbonzipper"
	"github.com/bookingcom/carbonapi/cfg"
	"github.com/facebookgo/pidfile"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

var BuildVersion = "(development version)"

func main() {
	err := zapwriter.ApplyConfig([]zapwriter.Config{cfg.GetDefaultLoggerConfig()})
	if err != nil {
		log.Fatal("Failed to initialize logger with default configuration")

	}
	logger := zapwriter.Logger("main")

	configFile := flag.String("config", "", "config file (yaml)")
	flag.Parse()

	err = pidfile.Write()
	if err != nil && !pidfile.IsNotConfigured(err) {
		log.Fatalln("error during pidfile.Write():", err)
	}

	expvar.NewString("GoVersion").Set(runtime.Version())

	if *configFile == "" {
		logger.Fatal("missing config file option")
	}

	fh, err := os.Open(*configFile)
	if err != nil {
		logger.Fatal("unable to read config file:",
			zap.Error(err),
		)
	}

	config, err1 := cfg.ParseZipperConfig(fh)
	if err1 != nil {
		logger.Fatal("failed to parse config",
			zap.String("config_path", *configFile),
			zap.Error(err1),
		)
	}
	fh.Close()

	if config.MaxProcs != 0 {
		runtime.GOMAXPROCS(config.MaxProcs)
	}

	if len(config.GetBackends()) == 0 {
		logger.Fatal("no Backends loaded -- exiting")
	}

	if err := zapwriter.ApplyConfig(config.Logger); err != nil {
		logger.Fatal("Failed to apply config",
			zap.Any("config", config.Logger),
			zap.Error(err),
		)
	}
	expvar.NewString("BuildVersion").Set(BuildVersion)
	logger.Info("starting carbonzipper",
		zap.String("build_version", BuildVersion),
		zap.Any("zipperConfig", config),
	)

	app, err := zipper.New(config, logger, BuildVersion)
	if err != nil {
		logger.Error("Error initializing app")
	}
	app.Start()
}
