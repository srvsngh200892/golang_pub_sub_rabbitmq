package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/srvnsngh200892/golang_pub_sub_rabbitmq/adapater"
)

func main() {

	adapater.Init("amqp://localhost")
	// start a publisher or subscriber based on argument
	if os.Args[1] == "subscriber_start" {
		subscriber()
	}
	else if os.Args[1] == "publisher_start" {
		publisher()
	}
	else {
		panic('undefined method call')
	}
}

func publisher() {
	//  publishes the message "hello,world" every 900 ms
	for {
		if err := adapater.Publish("queue1", []byte("hello,world")); err != nil {
			panic(err)
		}
		time.Sleep(900 * time.Millisecond)
	}
}

func subscriber() {

	// open channel
	data, close, err := adapater.Subscribe("queue1")
	if err != nil {
		panic(err)
	}
	defer close()
	forever := make(chan bool)

	go func() {
		// get the message
		for d := range data {
			
			string1, string2 := toString(d.Body)
			fmt.Println(time.Now().Format("01-02-2006 18:00:00"), "::", string1+string1)
			// acknowledge the message 
			d.Ack(false)
		}
	}()

	log.Printf(" [-**-] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func toString(b []byte) (int, int) {
	s := string(b)
	data := strings.Split(s, ",")
	data[0], data[1]
}
