#!/bin/bash

rm -rf demo_output.txt
go run sub/subscriber.go > demo_output.txt &

go run pub/rate_demo.go Pebbe 1000000
go run pub/rate_demo.go Pebbe 1000000
go run pub/rate_demo.go Pebbe 1000000
go run pub/rate_demo.go Pebbe 1000000
go run pub/rate_demo.go Pebbe 1000000

go run pub/rate_demo.go GoMQ 10000
go run pub/rate_demo.go GoMQ 10000
go run pub/rate_demo.go GoMQ 10000
go run pub/rate_demo.go GoMQ 10000
go run pub/rate_demo.go GoMQ 10000

go run pub/rate_demo.go GoCZMQ 1000000
go run pub/rate_demo.go GoCZMQ 1000000
go run pub/rate_demo.go GoCZMQ 1000000
go run pub/rate_demo.go GoCZMQ 1000000
go run pub/rate_demo.go GoCZMQ 1000000
go run pub/rate_demo.go GoCZMQ 1000000
go run pub/rate_demo.go GoCZMQ 1000000
go run pub/rate_demo.go GoCZMQ 1000000

# go run pub/rate_demo.go ZMQ4 1000000

cat demo_output.txt
kill %1
