package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	zmq "github.com/pebbe/zmq4"
)

func HasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func main() {
	subscriber, _ := zmq.NewSocket(zmq.SUB)
	defer subscriber.Close()
	subscriber.Connect("tcp://localhost:54345")
	subscriber.SetSubscribe("C")
	subscriber.SetSubscribe("END")
	subscriber.SetSubscribe("START")

	data := make(chan string)

	go func() {
		for {
			update_nbr := 0
			tstart := time.Now()
			started := false
		clear_counter:
			for {
				// Loop over all parts of a multi-part message
				parts := []string{}
				for {
					msg, e := subscriber.Recv(0)
					if e != nil {
						log.Println(e)
						return
					}
					if strings.Contains(msg, "START") {
						tstart = time.Now()
						started = true
						break
					}
					if strings.Contains(msg, "END") && started {
						dt := time.Since(tstart)
						fmt.Printf("Total message sets received: %9d in %v before %s.\n", update_nbr, dt, msg)
						break clear_counter
					}
					parts = append(parts, msg)
					more, e := subscriber.GetRcvmore()
					if e != nil {
						log.Println(e)
						return
					}
					if !more {
						data <- strings.Join(parts, ":")
						break
					}
				}
				update_nbr++
			}
		}
	}()

	ticker := time.NewTicker(300 * time.Second)
	nrx := 0
	nrx_total := 0
	for {
		select {
		case <-data:
			nrx += 1
		case <-ticker.C:
			nrx_total += nrx
			fmt.Printf("Received %8d messages (%12d total).\n", nrx, nrx_total)
			nrx = 0
		}
	}
}
