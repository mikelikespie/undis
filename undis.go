package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"bufio"
)

const (
	defaultListenAddr = "localhost:6378"
	defaultRedisAddr  = "localhost:6379"
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
	return 3
}



// Takes the stream after the * has been read
func parseMulti(reader *bufio.Reader) [][]byte {
	return [][]byte{}
}

func parseSingle(reader *bufio.Reader) [][]byte {

	cmdName, err := reader.ReadString(' ')

	if (err != nil) {
		fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
		return nil
	}


	fmt.Printf("Got cmd '%s\n'", cmdName)

	isBulk := cmds[cmdName] & REDIS_CMD_BULK == REDIS_CMD_BULK

	line, err := reader.ReadString('\n')
	if (err != nil || line[len(line)-1] != '\r') {
		fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
		return nil
	}

	// Trim the last character
	line = line[0:len(line)-1]

	fmt.Printf("Got rest of args '%s\n' isBulk '%v'", line, isBulk)

	// The line should end with crlf so check the last char is \r

	return [][]byte{}
}


// proxies from client to server
func inLoop(inReader *bufio.Reader, outWriter *bufio.Writer) {
	for {
		firstChar, err := inReader.ReadByte();

		if err != nil {
			fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
			return
		}

		var cmdArgs [][]byte

		if firstChar == '*' {
			cmdArgs = parseMulti(inReader)
		} else {
			inReader.UnreadByte();
			cmdArgs = parseSingle(inReader)
		}

		_ = cmdArgs

	}
}

func outLoop(inWriter *bufio.Writer, outReader *bufio.Reader) {
	for {
	}
}

func proxy(in net.Conn, redisAddr string) {
	out, err := net.Dial("tcp", "", redisAddr)

	if out == nil {
		fmt.Fprintf(os.Stderr, "outgoing connection failed: %v\n", err)
		return
	}

	fmt.Printf("Established outgoing connection\n")

	inReader := bufio.NewReader(in)
	outWriter := bufio.NewWriter(out)
	go inLoop(inReader, outWriter)

	inWriter := bufio.NewWriter(out)
	outReader := bufio.NewReader(in)
	go outLoop(inWriter, outReader)
}
