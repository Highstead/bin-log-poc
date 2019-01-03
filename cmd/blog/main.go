package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/highstead/bin-log-poc"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetFormatter(new(log.JSONFormatter))
	log.Info("starting bin-log-poc")

	// Parse flags.
	var (
		configdir = flag.String("c", "config", "config directory path")
	)
	flag.Parse()

	log.SetLevel(log.DebugLevel)

	secrets, err := binlog.ParseSecretsFile(*configdir)
	if err != nil {
		log.WithError(err).Panic("can't parse secrets file")
	}

	syncer := secrets.getSyncer()

	// Start an HTTP server so we can profile it with pprof.
	// See golang.org/pkd/net/http/pprof
	runtime.SetBlockProfileRate(1000) // 1 sample per 1000ns
	http.ListenAndServe(":8080", nil)
}
