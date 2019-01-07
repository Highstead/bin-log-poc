package binlog

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type kafkaConfig struct {
	Brokers struct {
		Aggregate []string `json:"_aggregate"`
		Local     []string `json:"_local"`
	} `json:"_brokers"`

	ClientKey  []byte `json:"client_key"`
	ClientCert []byte `json:"client_cert"`
}

type Secrets struct {
	Kafka    kafkaConfig `json:"_kafka"`
	CloudAgg kafkaConfig

	Zk struct {
		// Nodes is the list of Zookeeper nodes to create the consumer group with.
		Nodes []string `json:"_nodes"`
		// RootPath is the root of the path in Zookeeper for managing offsets and ids.
		RootPath string `json:"_root_path"`
		// TODO: remove once kafka offset tracking is working
		Cluster string `json:"_cluster"`
	} `json:"_zk"`

	Master MysqlConfig `json:"_master_mysql"`
}

func ParseSecretsFile(dir string) (*Secrets, error) {
	var filename string

	filename = filepath.Join(dir, "secrets.json")

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer file.Close()
	return ParseSecrets(file, dir)
}

func ParseSecrets(reader io.Reader, configDir string) (s *Secrets, err error) {
	s = &Secrets{}
	err = json.NewDecoder(reader).Decode(s)
	if err != nil {
		return
	}

	EnvironmentOverrides(s)

	return
}

func EnvironmentOverrides(secrets *Secrets) {
	entry := log.NewEntry(log.StandardLogger())

	if zkPeers := os.Getenv("ZOOKEEPER_PEERS"); zkPeers != "" {
		secrets.Zk.Nodes = strings.Split(zkPeers, ",")
		entry = entry.WithField("ZOOKEEPER_PEERS", zkPeers)
	}
	if brokers := os.Getenv("KAFKA"); brokers != "" {
		secrets.Kafka.Brokers.Local = strings.Split(brokers, ",")
		entry = entry.WithField("KAFKA", brokers)
	}
	if brokers := os.Getenv("KAFKA_CLOUD_AGGREGATE"); brokers != "" {
		secrets.CloudAgg.Brokers.Aggregate = strings.Split(brokers, ",")
		entry = entry.WithField("KAFKA_CLOUD_AGGREGATE", brokers)
		key := os.Getenv("KAFKA_CLOUD_AGGREGATE_CLIENT_KEY")
		cert := os.Getenv("KAFKA_CLOUD_AGGREGATE_CLIENT_CERT")
		secrets.CloudAgg.ClientCert = []byte(cert)
		secrets.CloudAgg.ClientKey = []byte(key)
	}
	if brokers := os.Getenv("KAFKA_AGGREGATE"); brokers != "" {
		secrets.Kafka.Brokers.Aggregate = strings.Split(brokers, ",")
		entry = entry.WithField("KAFKA_AGGREGATE", brokers)
	}
	if os.Getenv("KAFKA_CLIENT_CERT") != "" && os.Getenv("KAFKA_CLIENT_KEY") != "" {
		key := os.Getenv("KAFKA_CLIENT_KEY")
		cert := os.Getenv("KAFKA_CLIENT_CERT")
		secrets.Kafka.ClientCert = []byte(cert)
		secrets.Kafka.ClientKey = []byte(key)
	}

	entry.Info("environment overrides")
}

func saramaConfig(kConfig *kafkaConfig, isFile bool) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// NewHashPartitioner allows the hash of a message's key to be used to determine the
	// partition being produced to.
	config.Producer.Partitioner = sarama.NewHashPartitioner

	certPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}

	if len(kConfig.ClientKey) == 0 || len(kConfig.ClientCert) == 0 {
		return config, nil
	}

	// Configure SSL.
	var cert tls.Certificate
	if isFile {
		keyFile := string(kConfig.ClientKey)
		certFile := string(kConfig.ClientCert)
		log.WithFields(log.Fields{
			"Key":  keyFile,
			"Cert": certFile,
		}).Info("Parsing Client Key and Cert")

		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
	} else {
		cert, err = tls.X509KeyPair(kConfig.ClientCert, kConfig.ClientKey)
	}
	if err != nil {
		return nil, err
	}

	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      certPool,
	}
	return config, nil
}
