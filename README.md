# golang-zmq-comparison
Compare various ZMQ libraries for use in Golang

So far, I only intend to use the PUB/SUB feature. There will be tests to make sure I have the API correct,
and then a benchmark of some kind.

1. **pebbe/zmq4** at https://github.com/pebbe/zmq4 with [documentation](https://pkg.go.dev/github.com/pebbe/zmq4)
2. **goczmq** at https://github.com/zeromq/goczmq is the one we've been using in [Dastard](https://github.com/usnistgov/dastard)
3. **gomq** at https://github.com/zeromq/gomq (its README claims it's very immature)
4. **zmq4** at  https://github.com/go-zeromq/zmq4
