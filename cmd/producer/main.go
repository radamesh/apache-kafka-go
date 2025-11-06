package main

import (
	"bufio"
	"fmt"
	"os"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Digite a mensagem para ser publicado no kafka:")
	message, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading input:", err)
	} else {
		fmt.Printf("mensagem kafka: %s", message)

		deliveryChan := make(chan kafka.Event)
		producer := NewKafkaProducer()
		Publish(message, "test-topic", producer, nil, deliveryChan)
		// Publish("mesage enviada do GoLang 03", "test-topic", producer, []byte("transferencia1"), deliveryChan)
		
		go DeliveryReport(deliveryChan) // async
		producer.Flush(2000)
	}
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "localhost:9092",
		"delivery.timeout.ms": "0",
		"acks":                "all",
		"enable.idempotence":  "true",
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: 			[]byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: 			key,
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar a mensagem")
			} else {
				fmt.Println("Mensagem enviada: ", ev.TopicPartition)
				// anotar no banco de dados que a mensagem foi processado.
				// ex: confirma que uma transferencia bancaria ocorreu.
			}
		}
	}
}