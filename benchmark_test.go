package zmq_comparison

import (
	"context"
	"log"
	"net"
	"testing"

	"github.com/go-zeromq/zmq4"
	zmq "github.com/pebbe/zmq4"
)

const MessagesToSend = 100000

func BenchmarkPubOnly_PebbeZMQ(b *testing.B) {
	ctx, _ := zmq.NewContext()
	defer ctx.Term()

	//  Socket to talk to clients
	publisher, _ := ctx.NewSocket(zmq.PUB)
	defer publisher.Close()
	publisher.SetSndhwm(1100000)
	publisher.Bind("tcp://*:5568")

	msgA := [][]byte{
		[]byte("A"),
		[]byte("We don't want to see this"),
	}
	msgB := [][]byte{
		[]byte("B"),
		[]byte("We would like to see this"),
	}

	//  Now broadcast exactly 1M updates followed by END
	for i := 0; i < MessagesToSend; i++ {
		if _, err := publisher.SendMessage(msgA); err != nil {
			log.Fatal(err)
		}
		if _, err := publisher.SendMessage(msgB); err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkPubOnly_ZMQ4(b *testing.B) {
	pub := zmq4.NewPub(context.Background())
	defer pub.Close()

	err := pub.Listen("tcp://*:5569")
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
	for i := 0; i < MessagesToSend; i++ {
		//  Write two messages, each with an envelope and content
		if err := pub.Send(msgA); err != nil {
			log.Fatal(err)
		}
		if err := pub.Send(msgB); err != nil {
			log.Fatal(err)
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


