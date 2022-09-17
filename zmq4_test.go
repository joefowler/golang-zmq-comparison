package zmq_comparison

import (
	"context"
	"log"
	"strings"
	"time"
	"testing"

	"github.com/go-zeromq/zmq4"
)

func TestZMQ4(t *testing.T) {
	abort := make(chan bool)
	go zmq4_sub(abort)
	zmq4_pub(2*time.Second, abort)
}

func zmq4_pub(lifetime time.Duration, abort chan bool) {
	log.SetPrefix("psenvpub: ")

	// prepare the publisher
	pub := zmq4.NewPub(context.Background())
	defer pub.Close()

	err := pub.Listen("tcp://*:5563")
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}

	msgA := zmq4.NewMsgFrom(
		[]byte("A"),
		[]byte("We don't want to see this"),
	)
	msgB := zmq4.NewMsgFrom(
		[]byte("B"),
		[]byte("We would like to see this"),
	)
	done := time.NewTimer(lifetime)
	ticker := time.NewTicker(100*time.Millisecond)
	for {
		select {
		case <- done.C:
			abort <- true
			return
		case <- ticker.C:
			//  Write two messages, each with an envelope and content
			err = pub.Send(msgA)
			if err != nil {
				log.Fatal(err)
			}
			err = pub.Send(msgB)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func zmq4_sub(abort chan bool) {
	log.SetPrefix("psenvsub: ")

	//  Prepare our subscriber
	sub := zmq4.NewSub(context.Background())
	defer sub.Close()

	err := sub.Dial("tcp://localhost:5563")
	if err != nil {
		log.Fatalf("could not dial: %v", err)
	}

	err = sub.SetOption(zmq4.OptionSubscribe, "B")
	if err != nil {
		log.Fatalf("could not subscribe: %v", err)
	}

	mchan := make(chan zmq4.Msg)
	go func() {
		for {
			// Read envelope
			msg, err := sub.Recv()
			if err != nil {
				if strings.Contains(err.Error(), "context cancel") {
					return
				}
				log.Fatalf("could not receive message: %v", err)
			}
			mchan <- msg
		}
	}()
	for {
		select {
		case <- abort:
			return
		case msg := <-mchan:
			log.Printf("[%s] %s\n", msg.Frames[0], msg.Frames[1])
		}
	}
}
