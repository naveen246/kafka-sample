package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
)

type Message struct {
	User    string `json:"user"`
	Message string `json:"message"`
}

func main() {
	topic := "chat-room"
	brokers := []string{"localhost:9092"}
	admin := NewAdmin(brokers)
	defer admin.Close()
	if !admin.TopicExists(topic) {
		admin.CreateTopic(topic)
	}

	ctx := context.Background()
	_, err := admin.client.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return
	}
	username := ""
	fmt.Print("Enter your username: ")
	fmt.Scanln(&username)

	producer := NewProducer(brokers, topic)
	defer producer.Close()

	consumer := NewConsumer(brokers, topic)
	defer consumer.Close()
	go consumer.PrintMessages()

	fmt.Println("Connected. Press Ctrl+C to exit")
	reader := bufio.NewReader(os.Stdin)
	for {
		message, _ := reader.ReadString('\n')
		message = strings.TrimSpace(message)
		producer.SendMessage(username, message)
	}
}
