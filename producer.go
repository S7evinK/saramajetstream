package saramajetstream

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/nats-io/nats.go"
)

// MsgHeaderKey is the header used as the sarama.ConsumerMessage.Key
const MsgHeaderKey = "KEY"

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
	msg.Topic = strings.TrimPrefix(msg.Topic, p.stripPrefix)

	data, err := msg.Value.Encode()
	if err != nil {
		return 0, -1, err
	}

	if msg.Key != nil {
		key, err := msg.Key.Encode()
		if err != nil {
			return 0, -1, &sarama.ProducerError{
				Msg: msg,
				Err: err,
			}
		}
		// add a header with the given msg.Key
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   []byte(MsgHeaderKey),
			Value: key,
		})
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
