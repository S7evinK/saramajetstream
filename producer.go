package saramajetstream

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/nats-io/nats.go"
)

// Ensure JetStreamProducer implements sarama.SyncProducer
var _ sarama.SyncProducer = (*JetStreamProducer)(nil)

type JetStreamProducer struct {
	js nats.JetStreamContext
}

// nolint: deadcode
func NewJetStreamProducer(js nats.JetStreamContext) sarama.SyncProducer {
	return &JetStreamProducer{js: js}
}

// SendMessage implements sarama.SyncProducer
func (p *JetStreamProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	var data []byte

	switch val := msg.Value.(type) {
	case sarama.ByteEncoder:
		data = val
	case sarama.StringEncoder:
		data = []byte(val)
	default:
		errors := sarama.ProducerErrors{
			&sarama.ProducerError{
				Msg: msg,
				Err: fmt.Errorf("unknown encoding: %T", msg.Value),
			},
		}
		return 0, -1, errors
	}

	ack, err := p.js.Publish(msg.Topic, data)
	if err != nil {
		errors := sarama.ProducerErrors{
			&sarama.ProducerError{
				Msg: msg,
				Err: err,
			},
		}
		return 0, -1, errors
	}
	return 0, int64(ack.Sequence), nil
}

// SendMessages implements sarama.SyncProducer
func (p *JetStreamProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for _, msg := range msgs {
		_, _, err := p.SendMessage(msg)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close implements sarama.SyncProducer
func (p *JetStreamProducer) Close() error {
	return nil
}
