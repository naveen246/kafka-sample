package main

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdmin(t *testing.T) {
	topic := "test"
	brokers := []string{"localhost:9092"}
	a := NewAdmin(brokers)
	defer a.Close()

	createResp := a.CreateTopic(topic)
	assert.Nil(t, createResp.Err)
	assert.Equal(t, topic, createResp.Topic)
	assert.True(t, a.TopicExists(topic))

	deleteResp := a.DeleteTopic(topic)
	assert.Nil(t, deleteResp.Err)
	assert.Equal(t, topic, deleteResp.Topic)
	assert.False(t, a.TopicExists(topic))
}

func TestProducerConsumer(t *testing.T) {
	topic := "test"
	brokers := []string{"localhost:9092"}
	a := NewAdmin(brokers)
	defer a.Close()

	a.CreateTopic(topic)
	defer a.DeleteTopic(topic)

	producer := NewProducer(brokers, topic)
	defer producer.Close()

	consumer := NewConsumer(brokers, topic)
	defer consumer.Close()

	producer.SendMessage("user1", "message")
	fetches := consumer.client.PollFetches(context.Background())
	assert.Nil(t, fetches.Err())
	assert.False(t, fetches.Empty())
	assert.False(t, fetches.NumRecords() == 0)
	assert.Equal(t, topic, fetches.Records()[0].Topic)
}
