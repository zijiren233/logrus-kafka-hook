package logkafka

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type KafkaOptionFunc func(*sarama.Config)

func WithKafkaSASLEnable(enable bool) KafkaOptionFunc {
	return func(c *sarama.Config) {
		c.Net.SASL.Enable = enable
	}
}

func WithKafkaSASLHandshake(enable bool) KafkaOptionFunc {
	return func(c *sarama.Config) {
		c.Net.SASL.Handshake = enable
	}
}

func WithKafkaSASLUser(user string) KafkaOptionFunc {
	return func(c *sarama.Config) {
		c.Net.SASL.User = user
	}
}

func WithKafkaSASLPassword(password string) KafkaOptionFunc {
	return func(c *sarama.Config) {
		c.Net.SASL.Password = password
	}
}

func WithKafkaSASLMechanism(mechanism sarama.SASLMechanism) KafkaOptionFunc {
	return func(c *sarama.Config) {
		c.Net.SASL.Mechanism = mechanism
	}
}

func WithKafkaTLSEnable(enable bool) KafkaOptionFunc {
	return func(c *sarama.Config) {
		c.Net.TLS.Enable = enable
	}
}

func WithKafkaTLSConfig(config *tls.Config) KafkaOptionFunc {
	return func(c *sarama.Config) {
		c.Net.TLS.Config = config
	}
}

func NewKafkaClient(addrs []string, kafkaOpts ...KafkaOptionFunc) (sarama.Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("empty kafka brokers")
	}

	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.NoResponse
	conf.Producer.Compression = sarama.CompressionSnappy
	conf.Producer.Flush.Frequency = 500 * time.Millisecond
	for _, kof := range kafkaOpts {
		kof(conf)
	}

	client, err := sarama.NewClient(addrs, conf)
	if err != nil {
		return nil, err
	}

	return client, nil
}
