package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

func main() {

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}

	client, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	topics, _ := client.Topics()

	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}

		partitions, _ := client.Partitions(topic)

		consumer, err := client.ConsumePartition(topic, partitions[0], sarama.OffsetOldest)
		if nil != err {
			continue
		}
		select {
		case _ = <-consumer.Errors():
			continue
		case <-time.After(500 * time.Millisecond):
			continue
		case msg := <-consumer.Messages():
			fmt.Printf("kafka_oldest_message_age{topic=\"%s\"} %v\n", topic, msg.Timestamp.UnixNano())
		}
	}

	client.Close()
}
