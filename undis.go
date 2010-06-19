package main

import (
    "flag"
    "fmt"
    "net"
    "os"
    "./redisio"
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


// proxies from client to server
func cmdLoop(cmdReader *redisio.Reader, cmdWriter *redisio.Writer) {
    for {
        command, err := cmdReader.ReadCommand()
        if err != nil {
            fmt.Fprintf(os.Stderr, "redis read command failed: %v\n", err)
            return
        }

        fmt.Printf("Command: %v\n", command)

        err = cmdWriter.WriteCommand(command)
        if err != nil {
            fmt.Fprintf(os.Stderr, "redis write command failed: %v\n", err)
            return
        }
        cmdWriter.Flush()
    }
}

func replyLoop(replyReader *redisio.Reader, replyWriter *redisio.Writer) {
    for {
        reply, err := replyReader.ReadReply()
        if err != nil {
            fmt.Fprintf(os.Stderr, "redis read reply failed: %v\n", err)
            return
        }

        fmt.Printf("Reply: %v\n", reply)

        err = replyWriter.WriteReply(reply)
        if err != nil {
            fmt.Fprintf(os.Stderr, "redis write reply failed: %v\n", err)
            return
        }
        replyWriter.Flush()
    }
}

func proxy(in net.Conn, redisAddr string) {
    out, err := net.Dial("tcp", "", redisAddr)

    if out == nil {
        fmt.Fprintf(os.Stderr, "outgoing connection failed: %v\n", err)
        return
    }

    fmt.Printf("Established outgoing connection\n")

    cmdReader := redisio.NewReader(in)
    cmdWriter := redisio.NewWriter(out)

    replyReader := redisio.NewReader(out)
    replyWriter := redisio.NewWriter(in)
    go replyLoop(replyReader, replyWriter)
    go cmdLoop(cmdReader, cmdWriter)
}
