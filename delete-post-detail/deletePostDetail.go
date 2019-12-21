package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/djamboe/mtools-post-service/models"
	"log"
	"time"

	v1 "github.com/djamboe/mtools-post-service/pkg/service/v1"
	"github.com/streadway/amqp"
)

var (
	amqpURI = flag.String("amqp-consumer", "amqp://guest:guest@localhost:5672", "AMQP URI")
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func init() {
	flag.Parse()
	initAmqp()
}

var conn *amqp.Connection
var ch *amqp.Channel
var replies <-chan amqp.Delivery

func initAmqp() {
	var err error
	var q amqp.Queue

	conn, err = amqp.Dial(*amqpURI)
	failOnError(err, "Failed to connect to RabbitMQ")

	log.Printf("got Connection, getting Channel...")

	ch, err = conn.Channel()
	failOnError(err, "Failed to open a channel")

	log.Printf("got Channel, declaring Exchange (%s)", "go-test-exchange")

	err = ch.ExchangeDeclare(
		"delete-post-detail-exchange", // name of the exchange
		"direct",                      // type
		true,                          // durable
		false,                         // delete when complete
		false,                         // internal
		false,                         // noWait
		nil,                           // arguments
	)
	failOnError(err, "Failed to declare the Exchange")

	log.Printf("declared Exchange, declaring Queue (%s)", "go-test-queue")

	q, err = ch.QueueDeclare(
		"delete-post-data-detail-queue", // name, leave empty to generate a unique name
		true,                            // durable
		false,                           // delete when usused
		false,                           // exclusive
		false,                           // noWait
		nil,                             // arguments
	)
	failOnError(err, "Error declaring the Queue")

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		q.Name, q.Messages, q.Consumers, "go-test-key")

	err = ch.QueueBind(
		q.Name,                        // name of the queue
		"go-test-key-delete-detail",   // bindingKey
		"delete-post-detail-exchange", // sourceExchange
		false,                         // noWait
		nil,                           // arguments
	)
	failOnError(err, "Error binding to the Queue")

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", "go-amqp-example")

	replies, err = ch.Consume(
		q.Name,                    // queue
		"go-post-delete-consumer", // consumer
		true,                      // auto-ack
		false,                     // exclusive
		false,                     // no-local
		false,                     // no-wait
		nil,                       // args
	)
	failOnError(err, "Error consuming the Queue")
}

func main() {
	log.Println("Start consuming the Queue...")
	var count int = 1

	for r := range replies {

		log.Printf("Consuming reply number %d", count)
		postData := models.PostModelParam{}
		insertData := models.DeletePostModel{}
		json.Unmarshal(r.Body, &postData)
		insertData.IsDeleted = true
		insertData.UpdatedOn = time.Now()
		id := postData.DbId
		postController := v1.ServiceContainer().InjectPostController()
		_, err := postController.DeletePostDataDetailProcess(id, insertData)
		if err != nil {
			fmt.Printf(err.Error())
		}
		count++
	}
}
