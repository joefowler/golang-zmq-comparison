package zmq_comparison

import (
	"fmt"
	"log"
	"testing"
	"time"

	zmq "github.com/pebbe/zmq4"
)

const (
	//  We wait for 10 subscribers
	SUBSCRIBERS_EXPECTED = 10
)

func TestPebbePublish(t *testing.T) {
	for i := 0; i < SUBSCRIBERS_EXPECTED; i++ {
		go main_sub()
	}
	main_pub()
}

func main_sub() {
	//  First, connect our subscriber socket
	subscriber, _ := zmq.NewSocket(zmq.SUB)
	defer subscriber.Close()
	subscriber.Connect("tcp://localhost:5561")
	subscriber.SetSubscribe("")

	//  0MQ is so fast, we need to wait a while...
	time.Sleep(time.Second)

	//  Second, synchronize with publisher
	syncclient, _ := zmq.NewSocket(zmq.REQ)
	defer syncclient.Close()
	syncclient.Connect("tcp://localhost:5562")

	//  - send a synchronization request
	syncclient.Send("", 0)

	//  - wait for synchronization reply
	syncclient.Recv(0)

	//  Third, get our updates and report how many we got
	update_nbr := 0
	for {
		msg, e := subscriber.Recv(0)
		if e != nil {
			log.Println(e)
			break
		}
		if msg == "END" {
			break
		}
		update_nbr++
	}
	fmt.Printf("Received %d updates\n", update_nbr)
}

func main_pub() {
	ctx, _ := zmq.NewContext()
	defer ctx.Term()

	//  Socket to talk to clients
	publisher, _ := ctx.NewSocket(zmq.PUB)
	defer publisher.Close()
	publisher.SetSndhwm(1100000)
	publisher.Bind("tcp://*:5561")

	//  Socket to receive signals
	syncservice, _ := ctx.NewSocket(zmq.REP)
	defer syncservice.Close()
	syncservice.Bind("tcp://*:5562")

	//  Get synchronization from subscribers
	fmt.Println("Waiting for subscribers")
	for subscribers := 0; subscribers < SUBSCRIBERS_EXPECTED; subscribers++ {
		//  - wait for synchronization request
		syncservice.Recv(0)
		//  - send synchronization reply
		syncservice.Send("", 0)
	}
	//  Now broadcast exactly 1M updates followed by END
	fmt.Println("Broadcasting messages")
	for update_nbr := 0; update_nbr < 100000; update_nbr++ {
		publisher.Send("Rhubarb", 0)
	}

	publisher.Send("END", 0)
}
