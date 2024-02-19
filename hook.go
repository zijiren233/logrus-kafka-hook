package logkafka

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

var _ logrus.Hook = (*LogKafkaHook)(nil)

type HookFilter func(entry *logrus.Entry) bool

type LogKafkaHook struct {
	levels                       []logrus.Level
	topics                       []string
	producer                     sarama.AsyncProducer
	keyFormatter, valueFormatter logrus.Formatter
	filters                      []HookFilter
}

var _ logrus.Formatter = (*defaultKeyFormatter)(nil)

type defaultKeyFormatter struct{}

func (d *defaultKeyFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	return entry.Time.MarshalBinary()
}

type LogKafkaHookOptionFunc func(*LogKafkaHook)

func WithHookLevels(levels []logrus.Level) LogKafkaHookOptionFunc {
	return func(l *LogKafkaHook) {
		l.levels = levels
	}
}

func WithHookValueFormatter(formatter logrus.Formatter) LogKafkaHookOptionFunc {
	return func(l *LogKafkaHook) {
		l.valueFormatter = formatter
	}
}

func WithHookKeyFormatter(formatter logrus.Formatter) LogKafkaHookOptionFunc {
	return func(l *LogKafkaHook) {
		l.keyFormatter = formatter
	}
}

func hookMustHasFields(fields []string) HookFilter {
	return func(entry *logrus.Entry) bool {
		for _, v := range fields {
			if _, ok := entry.Data[v]; !ok {
				return false
			}
		}
		return true
	}
}

func WithHookMustHasFields(fields []string) LogKafkaHookOptionFunc {
	return func(l *LogKafkaHook) {
		l.filters = append(l.filters, hookMustHasFields(fields))
	}
}

func hookMustNotHasFields(fields []string) HookFilter {
	return func(entry *logrus.Entry) bool {
		for _, v := range fields {
			if _, ok := entry.Data[v]; ok {
				return false
			}
		}
		return true
	}
}

func WithHookMustNotHasFields(fields []string) LogKafkaHookOptionFunc {
	return func(l *LogKafkaHook) {
		l.filters = append(l.filters, hookMustNotHasFields(fields))
	}
}

func WithHookFilters(filters ...HookFilter) LogKafkaHookOptionFunc {
	return func(l *LogKafkaHook) {
		l.filters = append(l.filters, filters...)
	}
}

func NewLogKafkaHook(addrs, topics []string, kafkaOpts []KafkaOptionFunc, opts ...LogKafkaHookOptionFunc) (*LogKafkaHook, error) {
	client, err := NewKafkaClient(addrs, kafkaOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return NewLogKafkaHookFromClient(client, topics, opts...)
}

func NewLogKafkaHookFromClient(client sarama.Client, topics []string, opts ...LogKafkaHookOptionFunc) (*LogKafkaHook, error) {
	p, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}
	h := &LogKafkaHook{
		levels: logrus.AllLevels,
		valueFormatter: &logrus.JSONFormatter{
			TimestampFormat: time.DateTime,
		},
		keyFormatter: &defaultKeyFormatter{},
		topics:       topics,
		producer:     p,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h, nil
}

func (l *LogKafkaHook) Levels() []logrus.Level {
	return l.levels
}

func (l *LogKafkaHook) Fire(entry *logrus.Entry) error {
	for _, v := range l.filters {
		if !v(entry) {
			return nil
		}
	}

	var key sarama.ByteEncoder
	if l.keyFormatter == nil {
		b, err := entry.Time.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal time: %w", err)
		}
		key = sarama.ByteEncoder(b)
	} else {
		b, err := l.keyFormatter.Format(entry)
		if err != nil {
			return fmt.Errorf("failed to format entry: %w", err)
		}
		key = sarama.ByteEncoder(b)
	}

	var value sarama.ByteEncoder
	if l.valueFormatter == nil {
		b2, err := entry.Logger.Formatter.Format(entry)
		if err != nil {
			return fmt.Errorf("failed to format entry: %w", err)
		}
		value = sarama.ByteEncoder(b2)
	} else {
		b2, err := l.valueFormatter.Format(entry)
		if err != nil {
			return fmt.Errorf("failed to format entry: %w", err)
		}
		value = sarama.ByteEncoder(b2)
	}

	for _, topic := range l.topics {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   key,
			Value: value,
		}
		l.producer.Input() <- msg
	}
	return nil
}
