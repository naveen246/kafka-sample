package main

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"log/slog"
)

type Consumer struct {
	client *kgo.Client
	topic  string
}

func NewConsumer(brokers []string, topic string) *Consumer {
	groupId := uuid.New().String()
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupId),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	return &Consumer{
		client: client,
		topic:  topic,
	}
}

func (c *Consumer) PrintMessages() {
	ctx := context.Background()
	for {
		fetches := c.client.PollFetches(ctx)
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			var msg Message
			err := json.Unmarshal(record.Value, &msg)
			if err != nil {
				slog.Error("Error decoding message: %v\n", err)
				continue
			}
			slog.Info("%s: %s\n", msg.User, msg.Message)
		}
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
