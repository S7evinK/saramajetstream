package saramajetstream

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/nats-io/nats.go"
)

// Ensure JetStreamConsumer implements sarama.Consumer
var _ sarama.Consumer = (*JetStreamConsumer)(nil)

// Ensure partitionConsumer implements sarama.PartitionConsumer
var _ sarama.PartitionConsumer = (*partitionConsumer)(nil)

// JetStreamConsumer implements sarama.Consumer
type JetStreamConsumer struct {
	js                 nats.JetStreamContext
	stripPrefix        string
	partitionConsumers []*partitionConsumer
}

// NewJetStreamConsumer returns a sarama.Consumer
func NewJetStreamConsumer(js nats.JetStreamContext, stripPrefix string) sarama.Consumer {
	return &JetStreamConsumer{
		js:          js,
		stripPrefix: stripPrefix,
	}
}

// ConsumePartition implements sarama.Consumer
func (c *JetStreamConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	if partition != 0 {
		return nil, fmt.Errorf("unknown partition %d", partition)
	}

	var opt nats.SubOpt
	switch offset {
	case sarama.OffsetNewest:
		opt = nats.DeliverNew()
	case sarama.OffsetOldest:
		opt = nats.DeliverAll()
	default:
		if offset <= 0 {
			return nil, fmt.Errorf("offset should be greater 0")
		}
		opt = nats.StartSequence(uint64(offset))
	}

	pc := &partitionConsumer{
		subject:  strings.TrimPrefix(topic, c.stripPrefix),
		messages: make(chan *sarama.ConsumerMessage),
		errors:   make(chan *sarama.ConsumerError),
	}

	c.partitionConsumers = append(c.partitionConsumers, pc)

	var err error
	pc.sub, err = c.js.Subscribe(pc.subject, func(msg *nats.Msg) {
		meta, merr := msg.Metadata()
		if merr != nil {
			pc.errors <- &sarama.ConsumerError{
				Err:   merr,
				Topic: pc.subject,
			}
			return
		}
		pc.messages <- &sarama.ConsumerMessage{
			Headers:        toSaramaRecordHeader(msg.Header),
			Timestamp:      meta.Timestamp,
			BlockTimestamp: meta.Timestamp,
			Key:            []byte(msg.Header.Get(MsgHeaderKey)),
			Value:          msg.Data,
			Topic:          pc.subject,
			Partition:      0,
			Offset:         int64(meta.Sequence.Stream),
		}
	}, opt)
	return pc, err
}

// Topics implements sarama.Consumer
func (c *JetStreamConsumer) Topics() (result []string, err error) {
	for info := range c.js.StreamsInfo() {
		result = append(result, info.Config.Subjects...)
	}
	return
}

// Partitions implements sarama.Consumer
func (c *JetStreamConsumer) Partitions(topic string) ([]int32, error) {
	return []int32{0}, nil
}

// HighWaterMarks implements sarama.Consumer
func (c *JetStreamConsumer) HighWaterMarks() map[string]map[int32]int64 {
	result := make(map[string]map[int32]int64)
	for stream := range c.js.StreamsInfo() {
		m := make(map[int32]int64)
		m[0] = int64(stream.State.LastSeq + 1)
		result[stream.Config.Name] = m
	}
	return result
}

// Close implements sarama.Consumer
func (c *JetStreamConsumer) Close() error {
	for _, pc := range c.partitionConsumers {
		if err := pc.Close(); err != nil {
			return err
		}
	}
	return nil
}

type partitionConsumer struct {
	subject  string
	sub      *nats.Subscription
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

// AsyncClose implements sarama.PartitionConsumer
func (pc *partitionConsumer) AsyncClose() {
	_ = pc.Close()
}

// Close implements sarama.PartitionConsumer
func (pc *partitionConsumer) Close() error {
	close(pc.errors)
	close(pc.messages)
	return pc.sub.Unsubscribe()
}

// Messages implements sarama.PartitionConsumer
func (pc *partitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return pc.messages
}

// Errors implements sarama.PartitionConsumer
func (pc *partitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return pc.errors
}

// HighWaterMarkOffset implements sarama.PartitionConsumer
func (pc *partitionConsumer) HighWaterMarkOffset() int64 {
	ci, err := pc.sub.ConsumerInfo()
	if err != nil {
		return 1
	}
	return int64(ci.Delivered.Stream + 1)
}
