package zmq_comparison

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	zmq "github.com/pebbe/zmq4"
	czmq "github.com/zeromq/goczmq"
	"github.com/zeromq/gomq"
	"github.com/zeromq/gomq/zmtp"
)

const MessageSetsToSend = 1000000
const Endpoint = "tcp://*:54345"

func BenchmarkPubOnly_PebbeZMQ(b *testing.B) {
	log.Printf("Running benchmark PebbeZMQ")
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

	//  Now broadcast exactly `MessageSetsToSend` updates followed by END
	for i := 0; i < MessageSetsToSend; i++ {
		if _, err := publisher.SendMessage(msgA); err != nil {
			log.Fatal(err)
		}
		if _, err := publisher.SendMessage(msgB); err != nil {
			log.Fatal(err)
		}
		msg := fmt.Sprintf("Count %d", i)
		if _, err := publisher.Send(msg, 0); err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkPubOnly_ZMQ4(b *testing.B) {
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
	for i := 0; i < MessageSetsToSend; i++ {
		//  Write two messages, each with an envelope and content
		if err := pub.Send(msgA); err != nil {
			log.Fatal(err)
		}
		if err := pub.Send(msgB); err != nil {
			log.Fatal(err)
		}
		msg := zmq4.NewMsgFrom([]byte(fmt.Sprintf("Count %d", i)))
		pub.Send(msg)
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

func BenchmarkPubOnly_GoMQ(b *testing.B) {
	pub := NewPub(zmtp.NewSecurityNull())
	// defer pub.Close()
	Endpoint := "tcp://localhost:54345"
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
	for i := 0; i < MessageSetsToSend/100; i++ {
		//  Write two messages, each with an envelope and content

		if err := pub.SendMultipart(msgA); err != nil {
			log.Fatal(err)
		}
		if err := pub.SendMultipart(msgB); err != nil {
			log.Fatal(err)
		}
		msgC := []byte(fmt.Sprintf("Count %d", i))
		if err := pub.Send(msgC); err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkPubOnly_GoCZMQ(b *testing.B) {
	pubEndpoint := Endpoint
	pubSock, err := czmq.NewPub(pubEndpoint)
	if err != nil {
		panic(err)
	}

	defer pubSock.Destroy()
	pubSock.Bind(pubEndpoint)

	msgA := [][]byte{
		[]byte("A"),
		[]byte("We don't want to see this"),
	}
	msgB := [][]byte{
		[]byte("B"),
		[]byte("We would like to see this"),
	}

	//  Now broadcast exactly 1M updates followed by END
	for i := 0; i < MessageSetsToSend; i++ {
		if err := pubSock.SendMessage(msgA); err != nil {
			log.Printf("Failure %v on message A %d\n", err, i)
			// log.Fatal(err)
		}
		if err := pubSock.SendMessage(msgB); err != nil {
			log.Printf("Failure %v on message B %d\n", err, i)
			// log.Fatal(err)
		}
		broccoli := [][]byte{[]byte(fmt.Sprintf("Count %d", i))}
		if err := pubSock.SendMessage(broccoli); err != nil {
			log.Printf("Failure %v on message C %d\n", err, i)
		}
	}
}
