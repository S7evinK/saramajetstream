package saramajetstream

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/nats-io/nats.go"
)

// Ensure JetStreamProducer implements sarama.SyncProducer
var _ sarama.SyncProducer = (*JetStreamProducer)(nil)

// JetStreamProducer implements sarama.SyncProducer
type JetStreamProducer struct {
	js          nats.JetStreamContext
	stripPrefix string
}

// NewJetStreamProducer returns a sarama.SyncProducer
func NewJetStreamProducer(js nats.JetStreamContext, stripPrefix string) sarama.SyncProducer {
	return &JetStreamProducer{
		js:          js,
		stripPrefix: stripPrefix,
	}
}

// SendMessage implements sarama.SyncProducer
func (p *JetStreamProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	var data []byte
	msg.Topic = strings.TrimPrefix(msg.Topic, p.stripPrefix)

	switch val := msg.Value.(type) {
	case sarama.ByteEncoder:
		data = val
	case sarama.StringEncoder:
		data = []byte(val)
	default:
		pErr := &sarama.ProducerError{
			Msg: msg,
			Err: fmt.Errorf("unknown encoding: %T", msg.Value),
		}
		return 0, -1, pErr
	}
	ack, err := p.js.Publish(msg.Topic, data)
	if err != nil {
		pErr := &sarama.ProducerError{
			Msg: msg,
			Err: err,
		}
		return 0, -1, pErr
	}
	return 0, int64(ack.Sequence), nil
}

// SendMessages implements sarama.SyncProducer
func (p *JetStreamProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	errors := sarama.ProducerErrors{}
	for _, msg := range msgs {
		_, _, err := p.SendMessage(msg)
		if err != nil {
			errors = append(errors, err.(*sarama.ProducerError))
		}
	}
	if len(errors) == 0 {
		return nil
	}
	return errors
}

// Close implements sarama.SyncProducer
func (p *JetStreamProducer) Close() error {
	return nil
}
