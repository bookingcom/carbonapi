package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/bookingcom/carbonapi/pkg/app/carbonapi"
	"github.com/bookingcom/carbonapi/pkg/app/zipper"
	"github.com/bookingcom/carbonapi/pkg/cfg"
	"go.uber.org/zap"
)

// BuildVersion is provided to be overridden at build time. Eg. go build -ldflags -X 'main.BuildVersion=...'
var BuildVersion = "(development build)"

func main() {
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
	lg, err := apiConfig.LoggerConfig.Build()
	if err != nil {
		log.Fatalf("Failed to initiate logger: %s", err)
	}
	lg = lg.Named("carbonapi")

	lg.Info("starting carbonapi",
		zap.String("build_version", BuildVersion),
		zap.String("apiConfig", fmt.Sprintf("%+v", apiConfig)),
	)

	app, err := carbonapi.New(apiConfig, lg, BuildVersion)
	if err != nil {
		lg.Error("Error initializing app")
	}

	if apiConfig.EmbedZipper {
		lg.Info("starting embedded zipper")
		var zlg *zap.Logger
		app.Zipper, zlg = zipper.Setup(apiConfig.ZipperConfig, BuildVersion, "zipper", lg)
		// flush := trace.InitTracer(BuildVersion, "carbonzipper", zlg, app.Zipper.Config.Traces)
		// defer flush()
		go app.Zipper.Start(false, zlg)
	}

	flush := app.Start(lg)
	defer flush()
}
