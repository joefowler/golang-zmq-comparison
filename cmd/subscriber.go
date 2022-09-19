package main

import (
	"log"
	"fmt"
	"strings"
	"time"

	zmq "github.com/pebbe/zmq4"
)

func main() {
	subscriber, _ := zmq.NewSocket(zmq.SUB)
	defer subscriber.Close()
	subscriber.Connect("tcp://localhost:54345")
	subscriber.SetSubscribe("C")

	data := make(chan string)

	go func() {
		update_nbr := 0
		for {
			// Loop over all parts of a multi-part message
			parts := []string{}
			for {
				msg, e := subscriber.Recv(0)
				if e != nil {
					log.Println(e)
					return
				}
				parts = append(parts, msg)
				more,e := subscriber.GetRcvmore();
				if e != nil {
					log.Println(e)
					return
				}
				if !more {
					data <- strings.Join(parts, ":")
					break
				}
				// log.Println(msg)
				if msg == "END" {
					log.Println("Saw total of ", update_nbr, " messages.")
					return
				}
			}
			update_nbr++
		}
	}()

	ticker := time.NewTicker(time.Second)
	nrx := 0
	nrx_total := 0
	for {
		select {
		// case msg := <-data:
		case <-data:
			nrx += 1
			// if true {
			// 	fmt.Println(msg)
			// }
		case <-ticker.C:
			nrx_total += nrx
			fmt.Printf("Received %8d messages (%12d total).\n", nrx, nrx_total)
			nrx = 0
		}
	}
}
