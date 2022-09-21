package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/go-zeromq/zmq4"
	zmq "github.com/pebbe/zmq4"
	czmq "github.com/zeromq/goczmq"
	"github.com/zeromq/gomq"
	"github.com/zeromq/gomq/zmtp"
)

// Do we send a set of 3 messages in 5 parts per message set?
// If false, send only the "COUNT %d" message.
const filler_messages = false

func PubOnly_PebbeZMQ(Endpoint string, MessageSetsToSend int) {
	log.Printf("Running PebbeZMQ publisher with %d message sets", MessageSetsToSend)
	ctx, _ := zmq.NewContext()
	defer ctx.Term()

	//  Socket to talk to clients
	publisher, _ := ctx.NewSocket(zmq.PUB)
	defer publisher.Close()
	publisher.SetSndhwm(1100000)
	publisher.Bind(Endpoint)
	time.Sleep(300 * time.Millisecond)

	msgA := [][]byte{
		[]byte("A"),
		[]byte("We don't want to see this"),
	}
	msgB := [][]byte{
		[]byte("B"),
		[]byte("We would like to see this"),
	}

	time.Sleep(100 * time.Millisecond)
	if _, err := publisher.Send("START Pebbe", 0); err != nil {
		log.Printf("Failure %v on message START", err)
	}
	//  Now broadcast exactly `MessageSetsToSend` updates followed by END
	for i := 0; i < MessageSetsToSend; i++ {
		if filler_messages {
			if _, err := publisher.SendMessage(msgA); err != nil {
				log.Fatal(err)
			}
			if _, err := publisher.SendMessage(msgB); err != nil {
				log.Fatal(err)
			}
		}
		msg := fmt.Sprintf("Count %d", i)
		if _, err := publisher.Send(msg, 0); err != nil {
			log.Fatal(err)
		}
	}
	if _, err := publisher.Send("END Pebbe", 0); err != nil {
		log.Printf("Failure %v on message END", err)
	}
}

func PubOnly_ZMQ4(Endpoint string, MessageSetsToSend int) {
	log.Printf("Running ZMQ4 publisher with %d message sets", MessageSetsToSend)
	pub := zmq4.NewPub(context.Background())
	defer pub.Close()

	err := pub.Listen(Endpoint)
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
	time.Sleep(100 * time.Millisecond)
	start := zmq4.NewMsgFrom([]byte("START ZMQ4"))
	for i := 0; i < 5; i++ {
		time.Sleep(4 * time.Millisecond)
		if err := pub.Send(start); err != nil {
			log.Printf("Failure %v on message START", err)
		}
	}

	for i := 0; i < MessageSetsToSend; i++ {
		if filler_messages {
			//  Write two messages, each with an envelope and content
			if err := pub.Send(msgA); err != nil {
				log.Fatal(err)
			}
			if err := pub.Send(msgB); err != nil {
				log.Fatal(err)
			}
		}
		msg := zmq4.NewMsgFrom([]byte(fmt.Sprintf("Count %d", i)))
		pub.Send(msg)
	}
	time.Sleep(1000 * time.Millisecond)
	for i := 0; i < 5; i++ {
		end := zmq4.NewMsgFrom([]byte("END ZMQ4"))
		if err := pub.Send(end); err != nil {
			log.Printf("Failure %v on message END", err)
		}
	}
}

// PubSocket is a ZMQ_PUSH socket type.
// See: http://rfc.zeromq.org/spec:41
type PubSocket struct {
	*gomq.Socket
}

// NewPub accepts a zmtp.SecurityMechanism and returns
// a PubSocket as a gomq.Pub interface.
func NewPub(mechanism zmtp.SecurityMechanism) *PubSocket {
	return &PubSocket{
		Socket: gomq.NewSocket(false, zmtp.PubSocketType, nil, mechanism),
	}
}

// Bind accepts a zeromq endpoint and binds the
// pub socket to it. Currently the only transport
// supported is TCP. The endpoint string should be
// in the format "tcp://<address>:<port>".
func (s *PubSocket) Bind(endpoint string) (net.Addr, error) {
	return gomq.BindServer(s, endpoint)
}

// Connect accepts a zeromq endpoint and connects the
// client socket to it. Currently the only transport
// supported is TCP. The endpoint string should be
// in the format "tcp://<address>:<port>".
func (s *PubSocket) Connect(endpoint string) error {
	return gomq.ConnectClient(s, endpoint)
}

func PubOnly_GoMQ(Endpoint string, MessageSetsToSend int) {
	log.Printf("Running GoMQ publisher with %d message sets", MessageSetsToSend)
	pub := NewPub(zmtp.NewSecurityNull())
	// defer pub.Close()
	if addr, err := pub.Bind(Endpoint); err != nil {
		log.Printf("Tried to bind '%s' returned address %v", Endpoint, addr)
		log.Fatal(err)
	}

	msgA := [][]byte{
		[]byte("A"),
		[]byte("We don't want to see this"),
	}
	msgB := [][]byte{
		[]byte("B"),
		[]byte("We would like to see this"),
	}

	time.Sleep(100 * time.Millisecond)
	start := []byte("START GoMQ")
	if err := pub.Send(start); err != nil {
		log.Printf("Failure %v on message START", err)
	}
	for i := 0; i < MessageSetsToSend; i++ {
		if filler_messages {
			//  Write two messages, each with an envelope and content
			if err := pub.SendMultipart(msgA); err != nil {
				log.Fatal(err)
			}
			if err := pub.SendMultipart(msgB); err != nil {
				log.Fatal(err)
			}
		}
		msgC := []byte(fmt.Sprintf("Count %d", i))
		if err := pub.Send(msgC); err != nil {
			log.Fatal(err)
		}
	}
	end := []byte("END GoMQ")
	if err := pub.Send(end); err != nil {
		log.Printf("Failure %v on message END", err)
	}
}

func PubOnly_GoCZMQ(Endpoint string, MessageSetsToSend int) {
	log.Printf("Running GoCZMQ publisher with %d message sets", MessageSetsToSend)
	pubSock, err := czmq.NewPub(Endpoint)
	if err != nil {
		panic(err)
	}

	defer pubSock.Destroy()
	pubSock.Bind(Endpoint)

	msgA := [][]byte{
		[]byte("A"),
		[]byte("We don't want to see this"),
	}
	msgB := [][]byte{
		[]byte("B"),
		[]byte("We would like to see this"),
	}

	time.Sleep(100 * time.Millisecond)
	start := [][]byte{[]byte("START GoCZMQ")}
	for i := 0; i < 10; i++ {
		if err := pubSock.SendMessage(start); err != nil {
			log.Printf("Failure %v on message START", err)
		}
	}

	//  Now broadcast exactly 1M updates followed by END
	for i := 0; i < MessageSetsToSend; i++ {
		if filler_messages {
			if err := pubSock.SendMessage(msgA); err != nil {
				log.Printf("Failure %v on message A %d\n", err, i)
				// log.Fatal(err)
			}
			if err := pubSock.SendMessage(msgB); err != nil {
				log.Printf("Failure %v on message B %d\n", err, i)
				// log.Fatal(err)
			}
		}
		broccoli := [][]byte{[]byte(fmt.Sprintf("Count %d", i))}
		if err := pubSock.SendMessage(broccoli); err != nil {
			log.Printf("Failure %v on message C %d\n", err, i)
		}
	}
	end := [][]byte{[]byte("END GoCZMQ")}
	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)
		if err := pubSock.SendMessage(end); err != nil {
			log.Printf("Failure %v on message END", err)
		}
	}
}

func main() {
	library := os.Args[1]
	nMessageSets, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	switch library {
	case "Pebbe":
		PubOnly_PebbeZMQ("tcp://*:54345", nMessageSets)
	case "ZMQ4":
		PubOnly_ZMQ4("tcp://*:54345", nMessageSets)
	case "GoCZMQ":
		PubOnly_GoCZMQ("tcp://*:54345", nMessageSets)
	case "GoMQ":
		PubOnly_GoMQ("tcp://localhost:54345", nMessageSets)
	}
}
