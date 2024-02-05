package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Connect to RabbitMQ server
	conn, err := amqp.Dial("amqp://chat:chat@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Declare a fanout exchange
	err = ch.ExchangeDeclare(
		"chat_exchange", // Exchange name
		"fanout",        // Exchange type
		true,            // Durable
		false,           // Auto-deleted
		false,           // Internal
		false,           // No-wait
		nil,             // Arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}

	// Declare a unique queue for this instance
	queue, err := ch.QueueDeclare(
		"",    // Queue name (empty string for a unique queue name)
		false, // Durable
		true,  // Delete when unused
		true,  // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	// Bind the queue to the exchange
	err = ch.QueueBind(
		queue.Name,      // Queue name
		"",              // Routing key (not used for fanout exchanges)
		"chat_exchange", // Exchange name
		false,           // No-wait
		nil,             // Arguments
	)
	if err != nil {
		log.Fatalf("Failed to bind queue to exchange: %v", err)
	}

	// Create a WaitGroup to manage goroutines
	var wg sync.WaitGroup

	// Start consuming messages from the queue
	wg.Add(1)
	go func() {
		defer wg.Done()
		msgs, err := ch.Consume(
			queue.Name, // Queue name
			"",         // Consumer
			true,       // Auto-acknowledge messages
			false,      // Exclusive
			false,      // No-local
			false,      // No-wait
			nil,        // Arguments
		)
		if err != nil {
			log.Fatalf("Failed to register a consumer: %v", err)
		}
		for msg := range msgs {
			fmt.Printf("Received message: %s\n", msg.Body)
		}
	}()

	// Accept user input and publish it to the exchange
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		err := ch.Publish(
			"chat_exchange", // Exchange name
			"",              // Routing key (not used for fanout exchanges)
			false,           // Mandatory
			false,           // Immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(input),
			})
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Wait for the goroutines to finish
	wg.Wait()
}
