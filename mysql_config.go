package binlog

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

const DefaultMaxConnections = 100

type MysqlConfig struct {
	DB             string            `json:"_database,omitempty"`
	Flags          map[string]string `json:"_flags,omitempty"`
	Host           string            `json:"_host,omitempty"`
	Label          string            `json:"_label,omitempty"`
	MaxConnections int               `json:"_max_conns,omitempty"`
	Password       string            `json:"password,omitempty"`
	Port           int               `json:"_port,omitempty"`
	Proxy          bool              `json:"_proxy,omitempty"`
	SSL            string            `json:"_sslca,omitempty"`
	User           string            `json:"_username,omitempty"`

	tlsConfig string
}

func (m *MysqlConfig) Connect() (*sql.DB, error) {
	db, err := sql.Open("mysql", m.DataSourceString())
	if err != nil {
		return nil, errors.Wrapf(err, "cannot connect to db (%s)", m)
	} else if err := db.Ping(); err != nil {
		return nil, errors.Wrapf(err, "cannot ping db (%s)", m)
	} else {
		log.Infof("successfully connected to db (%s)", m.DB)
	}

	if m.MaxConnections == 0 {
		m.MaxConnections = DefaultMaxConnections
	}
	db.SetMaxOpenConns(m.MaxConnections)
	return db, nil
}

func (m *MysqlConfig) DataSourceString() string {
	netType := "tcp"
	if m.Proxy {
		netType = "proxy"
	}

	s := fmt.Sprintf("%s:%s@%s(%s:%d)/%s?%s",
		m.User,
		m.Password,
		netType,
		m.Host,
		m.Port,
		m.DB,
		m.EncodeFlags(),
	)

	return s
}

func (m *MysqlConfig) EncodeFlags() string {
	values := make(url.Values)
	values.Set("interpolateParams", "true")
	values.Set("parseTime", "true")
	values.Set("timeout", "30s")
	values.Set("readTimeout", "1m")
	values.Set("sql_mode", "'ONLY_FULL_GROUP_BY,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")
	values.Set("tx_isolation", "'READ-COMMITTED'")

	if m.tlsConfig != "" {
		values.Set("tls", m.tlsConfig)
	}

	for flag, value := range m.Flags {
		values.Set(strings.TrimPrefix(flag, "_"), value)
	}

	return values.Encode()
}

func (m *MysqlConfig) GetSyncer() *replication.BinlogSyncer {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     m.Host,
		Port:     uint16(m.Port),
		User:     m.User,
		Password: m.Password,
	}
	return replication.NewBinlogSyncer(cfg)
}

func (m *MysqlConfig) OpenCanal() context.Context {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", m.Host, m.Port)
	cfg.User = m.User
	cfg.Password = m.Password

	cfg.Dump.TableDB = "sales"
	cfg.Dump.Tables = []string{"sales"}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.WithError(err).Panic(err, "Unable to start canal")
	}
	c.Run()
	return c.Ctx()
}
