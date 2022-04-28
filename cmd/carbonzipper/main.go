package main

import (
	"expvar"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	zipper "github.com/bookingcom/carbonapi/app/carbonzipper"
	"github.com/bookingcom/carbonapi/cfg"
	"github.com/facebookgo/pidfile"
	"go.uber.org/zap"
)

var BuildVersion = "(development version)"

func main() {
	configFile := flag.String("config", "", "config file (yaml)")
	flag.Parse()

	err := pidfile.Write()
	if err != nil && !pidfile.IsNotConfigured(err) {
		log.Fatalln("error during pidfile.Write():", err)
	}

	expvar.NewString("GoVersion").Set(runtime.Version())

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

	expvar.NewString("BuildVersion").Set(BuildVersion)
	logger, err := config.Logger.Build()
	if err != nil {
		log.Fatalf("Failed to initiate logger: %s", err)
	}
	logger = logger.Named("carbonzipper")
	defer func() {
		if syncErr := logger.Sync(); syncErr != nil {
			log.Fatalf("could not sync the logger: %s", syncErr)
		}
	}()

	log.Printf("starting carbonzipper - build_version: %s, zipperConfig: %+v", BuildVersion, config)
	logger.Info("starting carbonzipper",
		zap.String("build_version", BuildVersion),
		zap.String("zipperConfig", fmt.Sprintf("%+v", config)),
	)

	app, err := zipper.New(config, logger, BuildVersion)
	if err != nil {
		logger.Error("Error initializing app")
	}
	flush := app.Start(logger)
	defer flush()
}
