package main

import (
	"context"
	"database/sql"
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/reportify-query/common"
	_ "github.com/go-sql-driver/mysql"
	binlog "github.com/highstead/bin-log-poc"
	log "github.com/sirupsen/logrus"
)

type Sale struct {
	value           int64
	discountPercent int64
}

func main() {
	log.SetFormatter(new(log.JSONFormatter))
	log.Info("starting mysql-background-inserter")

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
		log.WithError(err).Panic("unable to parse secrets file")
	}

	gracefulShutdown(secrets)
}

func gracefulShutdown(secrets *binlog.Secrets) {
	conn, err := secrets.Master.Connect()
	if err != nil {
		log.WithError(err).Panic("Unable to connect to mysql database")
	}

	ctx, cancel := context.WithCancel(context.Background())
	salesChannel := make(chan *Sale, 10)

	r := rand.New(rand.NewSource(44))
	log.Info("Beginning inserting data")
	go func() {
		for {
			dur := time.Duration(r.Int31n(2000)) * time.Millisecond
			select {
			case <-ctx.Done():
				log.Info("context done")
				return
			case <-time.After(dur):
				s := &Sale{
					value:           r.Int63n(10000),
					discountPercent: r.Int63n(3000),
				}
				salesChannel <- s
				err := inserter(ctx, conn, s)
				if err != nil {
					log.WithError(err).Println("unable to insert record")
				}
			}
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	log.Info("mysql-inserter is shutting down")
	cancel()
}

func inserter(ctx context.Context, conn *sql.DB, s *Sale) error {
	query := "INSERT INTO sales.sales(happened_at, currency, created_at, amount_displayed, discount_percent) VALUES(NOW(), 'CAD', NOW(), ?, ?);"
	_, err := conn.ExecContext(ctx, query, s.value/100, s.discountPercent/100)
	return err

}
