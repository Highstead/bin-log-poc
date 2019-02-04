package binlog

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

type EventHandler interface {
	// OnRotate occurs when the binary file is rotated because the previous file has filled up.
	OnRotate(roateEvent *replication.RotateEvent) error
	// OnTableChanged occurs when when the table structure changes (Data Manipulation Language)
	OnTableChanged(schema string, table string) error
	//OnDDL (Data Definition Language) occurs during Insert, delete, update and select
	OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error
	//OnRow (??) occurs as granular events between XIDEvents and isnt necessarily synced
	OnRow(e *canal.RowsEvent) error
	//   OnXID event is generated when a commit of a transaction modifies one or tables in the
	// XA (eXtendedArchitecture)-capable storage engine (InnoDb).
	OnXID(nextPos mysql.Position) error
	//  OnGTID is generated when a global transaction identifier is created by commiting a transaction
	OnGTID(gtid mysql.GTIDSet) error
	// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
	OnPosSynced(pos mysql.Position, force bool) error
	String() string
}

func NewLoggerEventHandler() EventHandler {
	return &loggerBlogEventHandler{}
}

type loggerBlogEventHandler struct {
}

func (h *loggerBlogEventHandler) OnRotate(rotateEvent *replication.RotateEvent) error {
	log.WithField("event", rotateEvent).Debug("Rotation Event Occured")
	return nil
}

func (h *loggerBlogEventHandler) OnTableChanged(schema string, table string) error {
	log.WithField("table", table).Debug("schema change", schema)
	return nil
}

func (h *loggerBlogEventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	log.WithField("pos", nextPos).Debug(queryEvent)
	return nil
}
func (h *loggerBlogEventHandler) OnRow(e *canal.RowsEvent) error {
	log.WithField("event", e).Debug("Row event")
	return nil
}

func (h *loggerBlogEventHandler) OnXID(p mysql.Position) error {
	log.WithField("position", p).Debug("XID Event")
	return nil
}
func (h *loggerBlogEventHandler) OnGTID(id mysql.GTIDSet) error {
	log.WithField("GTID", id).Debug("GTID Event")
	return nil
}
func (h *loggerBlogEventHandler) OnPosSynced(p mysql.Position, force bool) error {
	log.WithFields(log.Fields{
		"pos":   p,
		"force": force,
	}).Debug("Position Synced")
	return nil
}
func (h *loggerBlogEventHandler) String() string { return "binlogEventHandler" }

//kafkaBlogEventHandler emits the canal logs over kafka to be processed elsewhere
type kafkaBlogEventHandler struct {
	writer *kafka.Writer
	msgs   []kafka.Message
	sync   *sync.Mutex
}

func NewKafkaEventHandler(config *kafka.WriterConfig) EventHandler {
	return &kafkaBlogEventHandler{
		writer: kafka.NewWriter(*config),
		sync:   new(sync.Mutex),
	}
}

func (k *kafkaBlogEventHandler) AutoEmit(ctx context.Context, wFreq time.Duration) {
	go func() {
		for {
			select {
			case <-time.After(wFreq):
				k.WriteEvents(ctx)
			case <-ctx.Done():
				log.Info("Stopping kafka auto commiting")
				return
			}
		}
	}()
}

func (k *kafkaBlogEventHandler) WriteEvents(c context.Context) ([]kafka.Message, error) {
	ctx, cancel := context.WithTimeout(c, 10*time.Second)
	defer cancel()
	k.sync.Lock()
	msgs := k.msgs
	k.msgs = nil
	k.sync.Unlock()
	err := k.writer.WriteMessages(ctx, msgs...)
	return msgs, err
}

// OnRotate occurs when the binary file is rotated because the previous file has filled up.
func (kafkaBlogEventHandler) OnRotate(rotateEvent *replication.RotateEvent) error {
	log.WithField("event", rotateEvent).Debug("Rotation Event Occured")
	return nil
}

// OnTableChanged occurs when when the table structure changes (Data Manipulation Language)
func (k *kafkaBlogEventHandler) OnTableChanged(schema string, table string) error {
	return nil
}

//OnDDL (Data Definition Language) occurs during Insert, delete, update and select
func (k *kafkaBlogEventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {

	return nil
}

//OnRow (??) occurs as granular events between XIDEvents and isnt necessarily synced
func (k *kafkaBlogEventHandler) OnRow(e *canal.RowsEvent) error {
	msg := kafka.Message{
		Key:   []byte(`shard:GTID`),
		Value: []byte(fmt.Sprintf("%s %v", e.Action, e.Rows)),
		Time:  time.Now(),
	}
	k.sync.Lock()
	defer k.sync.Unlock()
	k.msgs = append(k.msgs, msg)
	return nil
}

//   OnXID event is generated when a commit of a transaction modifies one or tables in the
// XA (eXtendedArchitecture)-capable storage engine (InnoDb).
func (k *kafkaBlogEventHandler) OnXID(nextPos mysql.Position) error {
	return nil

}

//  OnGTID is generated when a global transaction identifier is created by commiting a transaction
func (k *kafkaBlogEventHandler) OnGTID(gtid mysql.GTIDSet) error {

	return nil
}

// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
func (k *kafkaBlogEventHandler) OnPosSynced(pos mysql.Position, force bool) error {
	return nil

}
func (kafkaBlogEventHandler) String() string {
	return "kafkaBlogEventHandler"
}
