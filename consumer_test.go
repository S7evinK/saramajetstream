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
	t.Cleanup(cleanup(t, s))

	js := connectServer(t, s.ClientURL())

	consumer := sjs.NewJetStreamConsumer(js, "")
	if consumer == nil {
		t.Fatalf("expected consumer to be not nil")
	}

	topics, err := consumer.Topics()
	if err != nil {
		t.Fatalf("expected no error")
	}

	if len(topics) == 0 {
		t.Fatalf("expected some topics, but got 0")
	}
	t.Logf("Topics: %+v", topics)
	if err := consumer.Close(); err != nil {
		t.Fatalf("failed to close consumer: %+v", err)
	}
}

func TestJetStreamConsumer_Partitions(t *testing.T) {
	s := startServer(t)
	t.Cleanup(cleanup(t, s))

	js := connectServer(t, s.ClientURL())

	consumer := sjs.NewJetStreamConsumer(js, "")
	if consumer == nil {
		t.Fatalf("expected consumer to be not nil")
	}

	partitions, err := consumer.Partitions("test")
	if err != nil {
		t.Fatalf("expected no error")
	}

	if len(partitions) == 0 {
		t.Fatalf("expected some partitions, but got 0")
	}
	t.Logf("Partitions: %+v", partitions)
	if err := consumer.Close(); err != nil {
		t.Fatalf("failed to close consumer: %+v", err)
	}
}

func TestJetStreamConsumer_HighWaterMarks(t *testing.T) {
	s := startServer(t)
	t.Cleanup(cleanup(t, s))

	js := connectServer(t, s.ClientURL())

	consumer := sjs.NewJetStreamConsumer(js, "")
	if consumer == nil {
		t.Fatalf("expected consumer to be not nil")
	}

	info, err := js.StreamInfo("test")
	if err != nil {
		t.Fatalf("unable to get stream info: %+v", err)
	}

	want := map[string]map[int32]int64{
		"test": {0: int64(info.State.LastSeq + 1)},
	}

	got := consumer.HighWaterMarks()

	if len(got) == 0 {
		t.Fatalf("expected some watermarks, but got 0")
	}
	t.Logf("HighWaterMarks: %+v", got)
	if err := consumer.Close(); err != nil {
		t.Fatalf("failed to close consumer: %+v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf(`expected "%+v", but got "%+v"`, want, got)
	}
}

func TestJetStreamConsumer_PartitionConsumer(t *testing.T) {
	s := startServer(t)
	t.Cleanup(cleanup(t, s))

	js := connectServer(t, s.ClientURL())

	consumer := sjs.NewJetStreamConsumer(js, "")
	if consumer == nil {
		t.Fatalf("expected consumer to be not nil")
	}
	producer := sjs.NewJetStreamProducer(js, "")

	hwm := consumer.HighWaterMarks()

	pc, err := consumer.ConsumePartition("test", 0, hwm["test"][0])
	if err != nil {
		t.Fatalf("unable to consume partition: %+v", err)
	}
	go func() {
		if pcErr := <-pc.Errors(); pcErr != nil {
			t.Errorf("unexpected error: %+v", pcErr)
			return
		}
	}()

	t.Cleanup(func() {
		pc.AsyncClose()
	})

	pcHwm := pc.HighWaterMarkOffset()
	if pcHwm != hwm["test"][0] {
		t.Fatalf("expected PartitionConsumer HighWaterMarkOffset (%d) to equal HighWatermarks (%d)", pcHwm, hwm["test"][0])
	}

	_, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic:     "test",
		Timestamp: time.Now(),
		Value:     sarama.StringEncoder("hello world"),
	})
	if err != nil {
		t.Fatalf("unable to send message: %+v", err)
	}

	msg := <-pc.Messages()
	want := "hello world"
	if string(msg.Value) != want {
		t.Fatalf(`expected message to be "%s" but got "%s"`, want, msg.Value)
	}

	if msg.Offset != offset || msg.Offset != hwm["test"][0] {
		t.Fatalf("unexpected offset, got %d, want %d", msg.Offset, offset)
	}
}

func TestJetStreamConsumer_Close(t *testing.T) {
	s := startServer(t)
	t.Cleanup(cleanup(t, s))

	js := connectServer(t, s.ClientURL())

	consumer := sjs.NewJetStreamConsumer(js, "")
	if consumer == nil {
		t.Fatalf("expected consumer to be not nil")
	}

	_, err := consumer.ConsumePartition("test", 0, sarama.OffsetNewest)
	if err != nil {
		t.Fatalf("unable to consume partition: %+v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Fatalf("unable to close consumer: %+v", err)
		}
	})
}
