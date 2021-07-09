package saramajetstream_test

import (
	"os"
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
	opts.Port = -1
	opts.JetStream = true
	return natsserver.RunServer(opts)
}

func connectServer(t *testing.T, url string) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("unable to connect to nats: %+v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("unable to get JetStream context: %+v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "test",
		Subjects: []string{"*"},
	})
	if err != nil {
		t.Fatalf("unable to add stream: %+v", err)
	}
	return nc, js
}

func cleanup(t *testing.T, nc *nats.Conn, s *server.Server) func() {
	return func() {
		t.Helper()
		nc.Close()
		s.Shutdown()
		if config := s.JetStreamConfig(); config != nil {
			if err := os.RemoveAll(config.StoreDir); err != nil {
				t.Fatal("unable to cleanup StoreDir", config.StoreDir)
			}
		}
	}
}

func TestJetStreamProducer_SendMessages(t *testing.T) {
	s := startServer(t)
	nc, js := connectServer(t, s.ClientURL())
	t.Cleanup(cleanup(t, nc, s))

	tests := []struct {
		name    string
		msgs    []*sarama.ProducerMessage
		wantErr bool
	}{
		{
			name: "send multiple messages",
			msgs: []*sarama.ProducerMessage{
				{
					Key:       sarama.StringEncoder("test"),
					Topic:     "test",
					Value:     sarama.StringEncoder("hello world"),
					Timestamp: time.Now(),
				},
				{
					Key:       sarama.StringEncoder("test"),
					Topic:     "test1",
					Value:     sarama.StringEncoder("hello world2"),
					Timestamp: time.Now(),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := sjs.NewJetStreamProducer(nc, js, "")
			err := p.SendMessages(tt.msgs)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SendMessages() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := p.Close(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestJetStreamProducer_SendMessage(t *testing.T) {
	s := startServer(t)
	nc, js := connectServer(t, s.ClientURL())
	t.Cleanup(cleanup(t, nc, s))

	i, err := js.StreamInfo("test")
	if err != nil {
		t.Fatalf("unable to get stream info: %+v", err)
	}

	p := sjs.NewJetStreamProducer(nc, js, "")

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
				Key:   sarama.StringEncoder("test"),
				Topic: "test",
				Value: sarama.StringEncoder("hello world"),
			},
			wantOffset: int64(i.State.LastSeq + 1),
		},
		{
			name: "byte message",
			msg: &sarama.ProducerMessage{
				Key:   sarama.StringEncoder("test"),
				Topic: "test",
				Value: sarama.ByteEncoder("hello world"),
			},
			wantOffset: int64(i.State.LastSeq + 2),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPartition, gotOffset, err := p.SendMessage(tt.msg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SendMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotPartition != tt.wantPartition {
				t.Fatalf("SendMessage() gotPartition = %v, want %v", gotPartition, tt.wantPartition)
			}
			if gotOffset != tt.wantOffset {
				t.Fatalf("SendMessage() gotOffset = %v, want %v", gotOffset, tt.wantOffset)
			}
		})
	}
	if err := p.Close(); err != nil {
		t.Error(err)
	}
}
