package main

import (
	"flag"
	"log"

	"github.com/bookingcom/carbonapi/pkg/app/zipper"
	"github.com/facebookgo/pidfile"
)

var BuildVersion string

func main() {
	configFile := flag.String("config", "", "config file (yaml)")
	flag.Parse()

	err := pidfile.Write()
	if err != nil && !pidfile.IsNotConfigured(err) {
		log.Fatalln("error during pidfile.Write():", err)
	}

	app, lg := zipper.Setup(*configFile, BuildVersion, "", nil)

	app.Start(true, lg)
}
