package saramajetstream_test

import (
	"reflect"
	"testing"
	"time"

	sjs "github.com/S7evinK/saramajetstream"
	"github.com/Shopify/sarama"
)

func TestJetStreamConsumer_Topics(t *testing.T) {
	s := startServer(t)
	defer s.Shutdown()
	js := connectServer(t)
	consumer := sjs.NewJetStreamConsumer(js, "")
	if consumer == nil {
		t.Errorf("expected consumer to be not nil")
	}

	topics, err := consumer.Topics()
	if err != nil {
		t.Errorf("expected no error")
	}

	if len(topics) == 0 {
		t.Errorf("expected some topics, but got 0")
	}
	t.Logf("Topics: %+v", topics)
	if err := consumer.Close(); err != nil {
		t.Errorf("failed to close consumer: %+v", err)
	}
}

func TestJetStreamConsumer_Partitions(t *testing.T) {
	s := startServer(t)
	defer s.Shutdown()
	js := connectServer(t)
	consumer := sjs.NewJetStreamConsumer(js, "")
	if consumer == nil {
		t.Errorf("expected consumer to be not nil")
	}

	partitions, err := consumer.Partitions("test")
	if err != nil {
		t.Errorf("expected no error")
	}

	if len(partitions) == 0 {
		t.Errorf("expected some partitions, but got 0")
	}
	t.Logf("Partitions: %+v", partitions)
	if err := consumer.Close(); err != nil {
		t.Errorf("failed to close consumer: %+v", err)
	}
}

func TestJetStreamConsumer_HighWaterMarks(t *testing.T) {
	s := startServer(t)
	defer s.Shutdown()
	js := connectServer(t)
	consumer := sjs.NewJetStreamConsumer(js, "")
	if consumer == nil {
		t.Errorf("expected consumer to be not nil")
	}

	info, err := js.StreamInfo("test")
	if err != nil {
		t.Errorf("unable to get stream info: %+v", err)
	}

	want := map[string]map[int32]int64{
		"test": {0: int64(info.State.LastSeq + 1)},
	}

	got := consumer.HighWaterMarks()

	if len(got) == 0 {
		t.Errorf("expected some watermarks, but got 0")
	}
	t.Logf("HighWaterMarks: %+v", got)
	if err := consumer.Close(); err != nil {
		t.Errorf("failed to close consumer: %+v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf(`expected "%+v", but got "%+v"`, want, got)
	}
}

func TestJetStreamConsumer_PartitionConsumer(t *testing.T) {
	s := startServer(t)
	defer s.Shutdown()
	js := connectServer(t)
	consumer := sjs.NewJetStreamConsumer(js, "")
	if consumer == nil {
		t.Errorf("expected consumer to be not nil")
	}
	producer := sjs.NewJetStreamProducer(js, "")

	pc, err := consumer.ConsumePartition("test", 0, sarama.OffsetNewest)
	if err != nil {
		t.Errorf("unable to consume partition: %+v", err)
	}

	_, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic:     "test",
		Timestamp: time.Now(),
		Value:     sarama.StringEncoder("hello world"),
	})
	if err != nil {
		t.Errorf("unable to send message: %+v", err)
	}

	msg := <-pc.Messages()
	want := "hello world"
	if string(msg.Value) != want {
		t.Fatalf(`expected message to be "%s" but got "%s"`, want, msg.Value)
	}

	// TODO: Fix failing test
	if msg.Offset != offset {
		t.Fatalf("unexpected offset, got %d, want %d", msg.Offset, offset)
	}

	if err := pc.Close(); err != nil {
		t.Errorf("unable to close PartitionConsumer: %+v", err)
	}
}
