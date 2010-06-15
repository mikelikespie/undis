package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"bufio"
	"strconv"
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


//Reads an integer, then reads some bytes
func parseBulk(reader *bufio.Reader) []byte {

	// read the expected $
	b, err := reader.ReadByte()
	if b != '$' || err != nil {
		fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
		panic("oh no")
		return nil
	}

	line, err := reader.ReadString('\n')
	if err != nil || line[len(line)-2] != '\r' {
		fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
		panic("oh no")
		return nil
	}

	line = line[0 : len(line)-2]

	numBytes, err := strconv.Atoi(line)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
		panic("oh no")
		return nil
	}

	buff := make([]byte, numBytes)

	nn, err := reader.Read(buff)

	if nn != len(buff) || err != nil {
		fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
		panic("oh no")
		return nil
	}

	//seek to the end of the line
	reader.ReadByte()
	reader.ReadByte()

	return buff
}

// Takes the stream after the * has been read
func parseMulti(reader *bufio.Reader) (string, [][]byte) {
	print("Got multi \n")

	line, err := reader.ReadString('\n')

	if err != nil || line[len(line)-2] != '\r' {
		panic("oh no")
		fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
		return "", nil
	}


	line = line[0 : len(line)-2] // trim off the \r


	numArgs, err := strconv.Atoi(line)
	if err != nil {
		panic("oh no")
		fmt.Fprintf(os.Stderr, "ohFormat not expected: %v\n", err)
		return "", nil
	} else if  numArgs < 1 {
		panic("oh no")
		fmt.Fprintf(os.Stderr, "bad format thought hwe'd have more than %d args\n", numArgs)
		return "", nil

	}

	numArgs--; // Subtract one off because we're going to be parsing the cmd Name

	cmdName := string(parseBulk(reader))


	return cmdName, [][]byte{}
}

func parseSingle(reader *bufio.Reader) (string, [][]byte) {

	cmdName, err := reader.ReadString(' ')

	if err != nil {
		panic("oh no")
		fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
		return "", nil
	}

	fmt.Printf("Got cmd '%s\n'", cmdName)

	isBulk := cmds[cmdName]&REDIS_CMD_BULK == REDIS_CMD_BULK

	line, err := reader.ReadString('\n')
	if err != nil || line[len(line)-2] != '\r' {
		fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
		return "", nil
	}

	// Trim the last character
	line = line[0 : len(line)-2]

	fmt.Printf("Got rest of args '%s\n' isBulk '%v'\n", line, isBulk)

	// The line should end with crlf so check the last char is \r

	return cmdName, [][]byte{}
}


// proxies from client to server
func inLoop(inReader *bufio.Reader, outWriter *bufio.Writer) {
	print("starting in loop\n")
	for {
		firstChar, err := inReader.ReadByte()
		var cmd string

		if err != nil {
			fmt.Fprintf(os.Stderr, "Format not expected: %v\n", err)
			return
		}

		var cmdArgs [][]byte

		if firstChar == '*' {
			cmd, cmdArgs = parseMulti(inReader)
		} else {
			inReader.UnreadByte()
			cmd, cmdArgs = parseSingle(inReader)
		}

		print("Got command " + cmd + "\n")

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

	//	inWriter := bufio.NewWriter(out)
	//	outReader := bufio.NewReader(in)
	//	go outLoop(inWriter, outReader)
}
