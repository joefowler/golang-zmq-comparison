package zmq_comparison

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	czmq "github.com/zeromq/goczmq"
)

func TestGOCZMQ(t *testing.T) {
	go main_wuserver(500 * time.Millisecond)
	main_wuclient(500 * time.Millisecond)
}

func main_wuclient(lifetime time.Duration) {
	pubEndpoint := "tcp://127.0.0.1:5556"
	totalTemperature := 0

	filter := "8"

	subSock, err := czmq.NewSub(pubEndpoint, filter)
	if err != nil {
		panic(err)
	}

	defer subSock.Destroy()

	fmt.Printf("Collecting updates from weather server for ZIP codes %s* ...\n", filter)
	// subSock.Connect(pubEndpoint)

	done := time.NewTicker(lifetime)
	recvchan := make(chan []byte)
	nmsg := 0
	nfail := 0
	go func() {
		for {
			msg, _, err := subSock.RecvFrame()
			// fmt.Println("Received: ", msg)
			nmsg += 1
			if err != nil {
				nfail += 1
				// panic(err)
			}
			recvchan <- msg
		}
	}()
	for {
		select {
		case <-done.C:
			fmt.Printf("Average temperature for zipcode %s* was %dF \n\n", filter,
				totalTemperature/nmsg)
			fmt.Printf("Received %d messages with %d failed\n", nmsg, nfail)
			return
		case msg := <-recvchan:

			weatherData := strings.Split(string(msg), " ")
			temperature, err := strconv.ParseInt(weatherData[1], 10, 64)
			if err == nil {
				totalTemperature += int(temperature)
				nmsg += 1
			}
		}
	}
}

func main_wuserver(lifetime time.Duration) {
	pubEndpoint := "tcp://127.0.0.1:5556"
	pubSock, err := czmq.NewPub(pubEndpoint)
	if err != nil {
		panic(err)
	}

	defer pubSock.Destroy()
	pubSock.Bind(pubEndpoint)

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	done := time.NewTimer(lifetime)

	nfail := 0
	ntotal := 0

	for {
		select {
		case <-ticker.C:
			zipcode := rand.Intn(100000)
			temperature := rand.Intn(215) - 85
			relHumidity := rand.Intn(50) + 10

			msg := fmt.Sprintf("%5.5d %d %d", zipcode, temperature, relHumidity)
			// fmt.Println("Message is ready: ", msg)
			ntotal += 1
			err := pubSock.SendFrame([]byte(msg), czmq.FlagNone)
			if err != nil {
				nfail += 1
				// fmt.Println("Failed to send:   ", msg)
				// panic(err)
			}
		case <-done.C:
			fmt.Printf("Failed to send %d of %d messages.\n", nfail, ntotal)
			return
		}
	}
}
