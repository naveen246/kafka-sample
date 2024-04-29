package main

import (
	"context"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"log/slog"
)

type Admin struct {
	client *kadm.Client
}

func NewAdmin(brokers []string) *Admin {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		panic(err)
	}
	adminClient := kadm.NewClient(client)
	return &Admin{adminClient}
}

func (a *Admin) TopicExists(topic string) bool {
	ctx := context.Background()

	topicDetails, err := a.client.ListTopics(ctx, topic)
	if err != nil {
		panic(err)
	}

	for _, metadata := range topicDetails {
		if metadata.Err == nil && topic == metadata.Topic {
			return true
		}
	}
	return false
}

func (a *Admin) CreateTopic(topic string) kadm.CreateTopicResponse {
	ctx := context.Background()
	resp, err := a.client.CreateTopic(ctx, 1, 1, nil, topic)
	if err != nil {
		slog.Error("Unable to create topic '%s': %s", topic, err)
		panic(err)
	}
	return resp
}

func (a *Admin) DeleteTopic(topic string) kadm.DeleteTopicResponse {
	ctx := context.Background()
	resp, err := a.client.DeleteTopic(ctx, topic)
	if err != nil {
		slog.Error(resp.ErrMessage)
		return resp
	}
	return resp
}

func (a *Admin) Close() {
	a.client.Close()
}
