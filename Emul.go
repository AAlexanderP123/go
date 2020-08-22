package main

import (
	"log"
	"github.com/streadway/amqp"
	"strconv"
	"encoding/json"
)

type messagejson struct{
    Id int
  	Dlc int
  	Data[8] int
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}


func main() {

	publisher, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ publisher\n")
	defer publisher.Close()

	chpub, err := publisher.Channel()
	failOnError(err, "Failed to open a channel publisher")
	defer chpub.Close()

	listener, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ listener\n")
	defer listener.Close()

	chlis, err := listener.Channel()
	failOnError(err, "Failed to open a channel listener")
	defer chlis.Close()

	qpub, err := chpub.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

	failOnError(err, "Failed to declare a queue publisher")


	qlis, err := chlis.QueueDeclare(
		"hello2", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

	msgs, err := chlis.Consume(
		qlis.Name, // queue
		"",     // consumer
		false,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	failOnError(err, "Failed to declare a queue listener")



	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			var msg messagejson
			err := json.Unmarshal(d.Body, &msg)
			//switch
			body := strconv.Itoa(msg.Id)
			err = chpub.Publish(
				"",     // exchange
				qpub.Name, // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(body),
			})
			log.Printf(" [x] Sent %s", body)
			failOnError(err, "Failed to publish a message")

			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
