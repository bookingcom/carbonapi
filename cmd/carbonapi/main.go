package main

import (
	"flag"
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
	configPath := flag.String("config", "", "Path to the config file.")
	versionFlag := flag.Bool("version", false, "Display version and exit.")
	flag.Parse()

	if *versionFlag {
		log.Println(BuildVersion)
		return
	}

	fh, err := os.Open(*configPath)
	if err != nil {
		log.Fatalf("failed to open config file: %f", err)
	}

	apiConfig, err := cfg.ParseAPIConfig(fh)
	if err != nil {
		log.Fatalf("failed to parse config file: %f", err)
	}
	err = fh.Close()
	if err != nil {
		log.Fatalf("failed to close config file: %f", err)
	}

	if apiConfig.MaxProcs != 0 {
		runtime.GOMAXPROCS(apiConfig.MaxProcs)
	}
	lg, err := apiConfig.LoggerConfig.Build()
	if err != nil {
		log.Fatalf("failed to initiate logger: %s", err)
	}

	lg.Info("starting carbonapi", zap.String("build_version", BuildVersion))

	app, err := carbonapi.New(apiConfig, lg, BuildVersion)
	if err != nil {
		lg.Error("error initializing app")
	}

	carbonapi.ProcessRequests(app, lg)

	app.Start(lg)
}
