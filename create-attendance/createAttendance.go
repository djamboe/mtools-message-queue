package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/djamboe/mtools-attendance-service/models"
	"log"

	v1 "github.com/djamboe/mtools-attendance-service/pkg/service/v1"
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
		"post-attendance-exchange", // name of the exchange
		"direct",                   // type
		true,                       // durable
		false,                      // delete when complete
		false,                      // internal
		false,                      // noWait
		nil,                        // arguments
	)
	failOnError(err, "Failed to declare the Exchange")

	log.Printf("declared Exchange, declaring Queue (%s)", "go-test-queue")

	q, err = ch.QueueDeclare(
		"post-attendance-queue", // name, leave empty to generate a unique name
		true,                    // durable
		false,                   // delete when usused
		false,                   // exclusive
		false,                   // noWait
		nil,                     // arguments
	)
	failOnError(err, "Error declaring the Queue")

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		q.Name, q.Messages, q.Consumers, "go-test-key")

	err = ch.QueueBind(
		q.Name,                     // name of the queue
		"go-test-key-attendance",   // bindingKey
		"post-attendance-exchange", // sourceExchange
		false,                      // noWait
		nil,                        // arguments
	)
	failOnError(err, "Error binding to the Queue")

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", "go-amqp-example")

	replies, err = ch.Consume(
		q.Name,                   // queue
		"go-attendance-consumer", // consumer
		true,                     // auto-ack
		false,                    // exclusive
		false,                    // no-local
		false,                    // no-wait
		nil,                      // args
	)
	failOnError(err, "Error consuming the Queue")
}

func main() {
	log.Println("Start consuming the Queue...")
	var count int = 1

	for r := range replies {
		var locationParam models.AttendanceParamModel
		log.Printf("Consuming reply number %d", count)

		json.Unmarshal(r.Body, &locationParam)
		attendanceController := v1.ServiceContainer().InjectAttendanceController()
		err := attendanceController.RecordLocation(locationParam)
		if err != nil {
			fmt.Printf(err.Error())
		}

		fmt.Println("reply from worker", count)
		count++
	}
}
