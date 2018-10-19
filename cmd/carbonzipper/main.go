package main

import (
	"expvar"
	"flag"
	"log"
	"os"
	"runtime"

	cfg "github.com/bookingcom/carbonapi/cfg"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	"github.com/bookingcom/carbonapi/app/zipper"
	"github.com/facebookgo/pidfile"
	//"github.com/uber/jaeger-client-go/config"
)

func main() {
	err := zapwriter.ApplyConfig([]zapwriter.Config{cfg.DefaultLoggerConfig})
	if err != nil {
		log.Fatal("Failed to initialize logger with default configuration")

	}
	logger := zapwriter.Logger("main")

	configFile := flag.String("config", "", "config file (yaml)")
	pidFile := flag.String("pid", "", "pidfile (default: empty, don't create pidfile)")
	if *pidFile != "" {
		pidfile.SetPidfilePath(*pidFile)
		err = pidfile.Write()
		if err != nil {
			log.Fatalln("error during pidfile.Write():", err)
		}
	}
	flag.Parse()

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

	if len(config.Backends) == 0 {
		logger.Fatal("no Backends loaded -- exiting")
	}

	if err := zapwriter.ApplyConfig(config.Logger); err != nil {
		logger.Fatal("Failed to apply config",
			zap.Any("config", config.Logger),
			zap.Error(err),
		)
	}

	zipper.StartCarbonZipper(config, logger)
}

