package main

import (
	"context"
	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/reportify-query/common"
	"github.com/highstead/bin-log-poc"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetFormatter(new(log.JSONFormatter))
	log.Info("starting bin-log-poc")

	// Parse flags.
	var (
		configdir = flag.String("c", "config", "config directory path")
		debug     = flag.String("d", "true", "debug mode")
	)
	flag.Parse()
	if strings.ToLower(*debug) == "true" {
		log.SetLevel(log.DebugLevel)
		log.SetOutput(os.Stdout)
		log.SetFormatter(common.LogFormatter{Formatter: new(log.TextFormatter)})
		log.Println("Logging in debug mode")
	} else {
		log.SetLevel(log.InfoLevel)
		log.SetFormatter(common.LogFormatter{Formatter: new(log.JSONFormatter)})
	}

	secrets, err := binlog.ParseSecretsFile(*configdir)
	if err != nil {
		log.WithError(err).Panic("can't parse secrets file")
	}
	eh := binlog.NewKafkaEventHandler(secrets.Kafka.WriteConfiger("test"))
	eh.AutoEmit(context.Background(), (time.Second))
	ctx := secrets.Master.OpenCanal(eh)
	log.Info("Canal Open")

	gracefulShutdown(ctx)
}

func gracefulShutdown(ctx context.Context) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case <-stop:
		log.Info("Recieved stop signal")
	case <-ctx.Done():
		log.WithField("ctx", ctx.Err()).Info("Context closed")
	}
}
