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
    var (
        listener net.Listener
        err      os.Error
    )
    flag.Parse()

    listenAddr := defaultListenAddr
    redisAddr := defaultRedisAddr

    // TODO: change this to take args

    fmt.Printf("Listening on %s\n", listenAddr)
    fmt.Printf("Connects to %s\n", redisAddr)

    if listener, err = net.Listen("tcp", listenAddr); listener == nil {
        fmt.Fprintf(os.Stderr, "Cannot listen: %v\n", err)
        os.Exit(1)
    }

    for {
        if conn, err := listener.Accept(); conn == nil {
            fmt.Fprintf(os.Stderr, "connect: %v\n", err)
            os.Exit(1)
        } else {
            fmt.Printf("Accepted connection\n")

            go proxy(conn, redisAddr)
        }
    }
}


// proxies from client to server
func cmdLoop(cmdReader *redisio.Reader, cmdWriter *redisio.Writer) {
    for {
        if command, err := cmdReader.ReadCommand(); err != nil {
            fmt.Fprintf(os.Stderr, "redis read command failed: %v\n", err)
            return
        } else {

            fmt.Printf("Command: %v\n", command)

            if err = cmdWriter.WriteCommand(command); err != nil {
                fmt.Fprintf(os.Stderr, "redis write command failed: %v\n", err)
                return
            }
            cmdWriter.Flush()
        }
    }
}

func replyLoop(replyReader *redisio.Reader, replyWriter *redisio.Writer) {
    for {
        if reply, err := replyReader.ReadReply(); err != nil {
            fmt.Fprintf(os.Stderr, "redis read reply failed: %v\n", err)
            return
        } else {

            fmt.Printf("Reply: %v\n", reply)

            if err = replyWriter.WriteReply(reply); err != nil {
                fmt.Fprintf(os.Stderr, "redis write reply failed: %v\n", err)
                return
            }
            replyWriter.Flush()
        }
    }
}

func proxy(in net.Conn, redisAddr string) {
    if out, err := net.Dial("tcp", "", redisAddr); out == nil {
        fmt.Fprintf(os.Stderr, "outgoing connection failed: %v\n", err)
        return
    } else {
        fmt.Printf("Established outgoing connection\n")

        cmdReader := redisio.NewReader(in)
        cmdWriter := redisio.NewWriter(out)

        replyReader := redisio.NewReader(out)
        replyWriter := redisio.NewWriter(in)
        go replyLoop(replyReader, replyWriter)
        go cmdLoop(cmdReader, cmdWriter)
    }
}
