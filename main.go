
package main

import (
        "fmt"
        "log"
        "os"
        "strings"
        "time"

        "github.com/srvsngh200892/golang_pub_sub_rabbitmq/adapter"
)

func main() {

        adapter.Init("amqp://localhost")
        // start a publisher or subscriber based on argument
        if os.Args[1] == "subscriber_start" {
                subscriber()
        } else if os.Args[1] == "publisher_start" {
                publisher()
        } else {
                fmt.Println("Please pass valid argument")
        }
}

func publisher() {
        //  publishes the message "hello,world" every 900 ms
        for {
                if err := adapter.Publish("queue1", []byte("hello,world")); err != nil {
                        panic(err)
                }
                time.Sleep(900 * time.Millisecond)
        }
}

func subscriber() {

        // open channel
        data, close, err := adapter.Subscribe("queue1")
        if err != nil {
                panic(err)
        }
        defer close()
        forever := make(chan bool)

        go func() {
                // get the message
                for d := range data {

                        string1, string2 := toString(d.Body)
                        fmt.Println(time.Now().Format("01-02-2006 18:00:00"), "::", string1+string2)
                        // acknowledge the message
                        d.Ack(false)
                }
        }()

        log.Printf(" [-**-] Waiting for messages. To exit press CTRL+C")
        <-forever
}

func toString(b []byte) (string, string) {
        s := string(b)
        data := strings.Split(s, ",")
        return data[0], data[1]
}
