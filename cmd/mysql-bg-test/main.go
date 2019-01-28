package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
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

	cancel := simpleProgram(secrets)
	gracefulShutdown(cancel)

}

func gracefulShutdown(cancel context.CancelFunc) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	log.Info("mysql-inserter is shutting down")
	cancel()
}

type Sale struct {
	value           int64
	discountPercent int64
}

func (s *Sale) String() string {
	return fmt.Sprintf("Sale value: %d, discount: %d", s.value, s.discountPercent)
}

func simpleProgram(secrets *binlog.Secrets) context.CancelFunc {
	secrets.Master.MultiStatement = true
	conn, err := secrets.Master.Connect()
	if err != nil {
		log.WithError(err).Panic("Unable to connect to mysql database")
	}

	ctx, cancel := context.WithCancel(context.Background())
	insertChannel := make(chan *Sale, 10)
	transChannel := make(chan *Sale, 10)
	delChannel := make(chan bool, 10)

	r := rand.New(rand.NewSource(44))

	go func() {
		for {
			dur := time.Duration(r.Int31n(2000)) * time.Millisecond
			select {
			case <-ctx.Done():
				log.Info("context done")
				return
			case <-time.After(dur):
				insertChannel <- &Sale{
					value:           r.Int63n(10000),
					discountPercent: r.Int63n(3000),
				}
			}
		}
	}()

	go func() {
		for {
			dur := time.Duration(r.Int31n(2000)) * time.Millisecond
			select {
			case <-ctx.Done():
				log.Info("context done")
				return
			case <-time.After(dur):
				transChannel <- &Sale{
					value:           r.Int63n(10000),
					discountPercent: r.Int63n(3000),
				}
			}
		}
	}()

	go func() {
		for {
			dur := time.Duration(r.Int31n(2000)) * time.Millisecond
			select {
			case <-ctx.Done():
				log.Info("context done")
				return
			case <-time.After(dur):
				delChannel <- true
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info("context done")
				return
			case s := <-insertChannel:
				err := inserter(ctx, conn, s)
				if err != nil {
					log.WithField("type", "insert").WithError(err).Println("unable to insert record")
				} else {
					log.WithField("type", "insert").Debug(s)
				}
			case s := <-transChannel:
				err := transInsert(ctx, conn, s)
				if err != nil {
					log.WithField("type", "trans").WithError(err).Println("unable to trans-insert record")
				} else {
					log.WithField("type", "trans").Debug(s)
				}
			case <-delChannel:
				err := deleter(ctx, conn)
				if err != nil {
					log.WithField("type", "del").WithError(err).Println("unable to delete record")
				} else {
					log.WithField("type", "del").Debug("delete")
				}
			}
		}
	}()

	return cancel

}

func inserter(ctx context.Context, conn *sql.DB, s *Sale) error {
	query := "INSERT INTO sales.sales(happened_at, currency, created_at, amount_displayed, discount_percent) VALUES(NOW(), 'CAD', NOW(), ?, ?);"
	_, err := conn.ExecContext(ctx, query, s.value/100, s.discountPercent/100)
	return err
}

func transInsert(ctx context.Context, conn *sql.DB, s *Sale) error {
	query := `
		START TRANSACTION;
		SET @saleNum = (SELECT MAX(id)+1 FROM sales.sales);
		INSERT INTO sales.sales(id, happened_at, currency, created_at, amount_displayed, discount_percent) VALUES(@saleNum, NOW(), 'CAD', NOW(), ?, ?);
		COMMIT;
	`
	_, err := conn.ExecContext(ctx, query, s.value/100, s.discountPercent/100)
	return err
}

func deleter(ctx context.Context, conn *sql.DB) error {
	query := `
		START TRANSACTION;
		SET @delNum = (SELECT MAX(id) FROM sales.sales);
		DELETE FROM sales.sales WHERE id = @delNum;
		COMMIT;
	`
	_, err := conn.ExecContext(ctx, query)
	return err
}
