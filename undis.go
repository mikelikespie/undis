package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"io"
)

const (
	defaultListenAddr = "localhost:6378"
	defaultRedisAddr = "localhost:6379"
)


func main() {
	flag.Parse()


	listenAddr := defaultListenAddr
	redisAddr := defaultRedisAddr

	// TODO: change this to take args

	fmt.Printf("Listening on %s\n", listenAddr)
	fmt.Printf("Connects to %s\n", redisAddr)

	listener, err := net.Listen("tcp", listenAddr)
	if listener == nil {
		fmt.Fprintf(os.Stderr, "Cannot listen: %v\n", err)
		os.Exit(1)
	}

	for {
		conn, err := listener.Accept()

		if conn == nil {
			fmt.Fprintf(os.Stderr, "connect: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Accepted connection\n")

		go proxy(conn, redisAddr)
	}
}




func readCmd(in net.Conn) int {
	foo := cmds["del"]

	fmt.Printf("%d\n", foo)
	return foo
}

// proxies from in to out
func inLoop(in net.Conn, out net.Conn) {
	for {
		readCmd(in)

	}
}

func proxy(in net.Conn, redisAddr string) {
	out, err := net.Dial("tcp", "", redisAddr)

	if out == nil {
		fmt.Fprintf(os.Stderr, "outgoing connection failed: %v\n", err)
		return
	}

	fmt.Printf("Established outgoing connection\n")

	go io.Copy(in, out)
	go io.Copy(out, in)
}
