
package adapter

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

var connect *amqp.Connection

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func Init(c string) {
	var err error
	// Initialize connection to the rabbitmq server
	connect, err = amqp.Dial(c)
	if err != nil {
		log.Fatalf("could not connect to rabbitmq: %v", err)
		panic(err)
	}
}

func Publish(q string, msg []byte) error {
	// create a channel through which we publish
	ch, err := connect.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
	            "logs",   // name
	            "fanout", // type
	            true,     // durable
	            false,    // auto-deleted
	            false,    // internal
	            false,    // no-wait
	            nil,      // arguments
	    )
	    failOnError(err, "Failed to declare an exchange")

	// create the payload with the message that we specify in the arguments
	payload := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         msg,
	}

	// publish the message to the queue specified in the arguments
	if err := ch.Publish("logs", q, false, false, payload); err != nil {
		return fmt.Errorf("[Publisher] failed to publish to queue %v", err)
	}

	return nil
}

func Subscribe(qName string) (<-chan amqp.Delivery, func(), error) {
	// create a channel through which we publish
	ch, err := connect.Channel()
	if err != nil {
		return nil, nil, err
	}

	err = ch.ExchangeDeclare(
                "logs",   // name
                "fanout", // type
                true,     // durable
                false,    // auto-deleted
                false,    // internal
                false,    // no-wait
                nil,      // arguments
        )
    failOnError(err, "Failed to declare an exchange")

    q, err := ch.QueueDeclare(
            qName,    // name
            false, // durable
            false, // delete when usused
            true,  // exclusive
            false, // no-wait
            nil,   // arguments
    )
    failOnError(err, "Failed to declare a queue")

    err = ch.QueueBind(
            q.Name, // queue name
            "",     // routing key
            "logs", // exchange
            false,
            nil)
    failOnError(err, "Failed to bind a queue")

	// assert that the queue exists (creates a queue if it doesn't)

	// create a channel in go, through which incoming messages will be received
	c, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	// return the created channel
	return c, func() { ch.Close() }, err
}
