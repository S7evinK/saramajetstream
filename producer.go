package saramajetstream

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/nats-io/nats.go"
)

// MsgHeaderKey is the header used as the sarama.ConsumerMessage.Key
const MsgHeaderKey = "$_SARAMA_NATS_KEY"

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
	data, err := msg.Value.Encode()
	if err != nil {
		return 0, -1, err
	}

	// create a new message to use headers
	nMsg := nats.NewMsg(strings.TrimPrefix(msg.Topic, p.stripPrefix))
	nMsg.Header = toNATSHeader(msg.Headers)
	nMsg.Data = data

	if msg.Key != nil {
		key, err := msg.Key.Encode()
		if err != nil {
			return 0, -1, &sarama.ProducerError{
				Msg: msg,
				Err: err,
			}
		}
		// set header MsgHeaderKey to the sarama.ProducerMessage.Key
		nMsg.Header.Set(MsgHeaderKey, string(key))
	}

	ack, err := p.js.PublishMsg(nMsg)
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

// convert sarama.RecordHeader to nats.Header
func toNATSHeader(headers []sarama.RecordHeader) nats.Header {
	result := make(nats.Header)
	for _, header := range headers {
		result[string(header.Key)] = []string{string(header.Value)}
	}
	return result
}

// convert nats.Header to sarama.RecordHeader
func toSaramaRecordHeader(headers nats.Header) []*sarama.RecordHeader {
	result := make([]*sarama.RecordHeader, len(headers))
	i := 0
	for key, value := range headers {
		result[i] = &sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value[0]),
		}
		i++
	}
	return result
}
