package main

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
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

func (a *Admin) CreateTopic(topic string) {
	ctx := context.Background()
	_, err := a.client.CreateTopic(ctx, 3, 3, nil, topic)
	if err != nil {
		fmt.Printf("Unable to create topic '%s': %s", topic, err)
		panic(err)
	}
}

func (a *Admin) Close() {
	a.client.Close()
}
