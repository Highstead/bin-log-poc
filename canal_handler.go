package binlog

import (
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
