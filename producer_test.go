package saramajetstream_test

import (
	"testing"
	"time"

	sjs "github.com/S7evinK/saramajetstream"
	"github.com/Shopify/sarama"
	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func startServer(t *testing.T) *server.Server {
	t.Helper()
	opts := &natsserver.DefaultTestOptions
	opts.JetStream = true
	return natsserver.RunServer(opts)
}

func connectServer(t *testing.T) nats.JetStreamContext {
	t.Helper()
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Errorf("unable to connect to nats: %+v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Errorf("unable to get JetStream context: %+v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "test",
		Subjects: []string{"*"},
	})
	if err != nil {
		t.Errorf("unable to add stream: %+v", err)
	}
	return js
}

func TestJetStreamProducer_SendMessages(t *testing.T) {
	s := startServer(t)
	defer s.Shutdown()
	js := connectServer(t)

	tests := []struct {
		name    string
		msgs    []*sarama.ProducerMessage
		wantErr bool
	}{
		{
			name: "send multiple messages",
			msgs: []*sarama.ProducerMessage{
				{
					Topic:     "test",
					Value:     sarama.StringEncoder("hello world"),
					Timestamp: time.Now(),
				},
				{
					Topic:     "test1",
					Value:     sarama.StringEncoder("hello world2"),
					Timestamp: time.Now(),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := sjs.NewJetStreamProducer(js, "")
			err := p.SendMessages(tt.msgs)
			if (err != nil) != tt.wantErr {
				t.Errorf("SendMessages() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

type dummyEncoder string

func (s dummyEncoder) Encode() ([]byte, error) {
	return []byte(s), nil
}

func (s dummyEncoder) Length() int {
	return len(s)
}

func TestJetStreamProducer_SendMessage(t *testing.T) {
	s := startServer(t)
	defer s.Shutdown()
	js := connectServer(t)

	i, err := js.StreamInfo("test")
	if err != nil {
		t.Errorf("unable to get stream info: %+v", err)
	}

	tests := []struct {
		name          string
		msg           *sarama.ProducerMessage
		wantPartition int32
		wantOffset    int64
		wantErr       bool
	}{
		{
			name: "string message",
			msg: &sarama.ProducerMessage{
				Topic: "test",
				Value: sarama.StringEncoder("hello world"),
			},
			wantOffset: int64(i.State.LastSeq + 1),
		},
		{
			name: "byte message",
			msg: &sarama.ProducerMessage{
				Topic: "test",
				Value: sarama.ByteEncoder("hello world"),
			},
			wantOffset: int64(i.State.LastSeq + 2),
		},
		{
			name: "unknown encoder",
			msg: &sarama.ProducerMessage{
				Topic: "test",
				Value: dummyEncoder("testing"),
			},
			wantErr:    true,
			wantOffset: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := sjs.NewJetStreamProducer(js, "")
			gotPartition, gotOffset, err := p.SendMessage(tt.msg)
			if (err != nil) != tt.wantErr {
				t.Errorf("SendMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotPartition != tt.wantPartition {
				t.Errorf("SendMessage() gotPartition = %v, want %v", gotPartition, tt.wantPartition)
			}
			if gotOffset != tt.wantOffset {
				t.Errorf("SendMessage() gotOffset = %v, want %v", gotOffset, tt.wantOffset)
			}
		})
	}
}
