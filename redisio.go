package redisio

import (
    "bufio"
    "io"
    "os"
    "fmt"
    "strconv"
    "strings"
    "bytes"
)

const (
    Test      = 3
    LineDelim = "\r\n"
)


type RedisIOError struct {
    Msg   string
    Error os.Error
}

func (e *RedisIOError) String() string {
    if e.Error == nil {
        return "redis protocol error " + e.Msg
    }

    return "redis protocol error caught: " + e.Msg + "\n\t[" + e.Error.String() + "]"
}


// origError can be nil
func newError(origError os.Error, msgFmt string, a ...interface{}) (err os.Error) {
    return &RedisIOError{fmt.Sprintf(msgFmt, a), origError}
}


/* Args is just a slice of vals, and Name is the first index */
type Command struct {
    Name []byte
    Args [][]byte
    vals [][]byte
}

func (rc *Command) String() string {
    return fmt.Sprintf("redisio.Command(Name='%s', Args={'%s'})",
        rc.Name, bytes.Join(rc.Args, []byte("', '")))
}

type Reply struct {
    code byte
    vals [][]byte
}

func (rp *Reply) String() string {
    return fmt.Sprintf("redisio.Reply(code='%c', vals={'%s'})",
        rp.code, bytes.Join(rp.vals, []byte("', '")))
}

type Reader struct {
    rd *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
    rr := new(Reader)
    rr.rd = bufio.NewReader(rd)
    return rr
}


/*
 * Note: When there's an error-type reply (one that starts with '-')
 * we don't throw an error
 */
func (rr *Reader) ReadReply() (rp *Reply, err os.Error) {
    rp = new(Reply)
    rp.code, err = rr.peekByte()
    if err != nil {
        return nil, err
    }

    switch rp.code {
    case '-', '+', ':':
        ln, err := rr.readLineBytes()
        if err != nil {
            return nil, newError(err, "error reading line bytes")
        }
        //print(string(ln))
        rp.vals = [][]byte{ln[1:]}
        break
    case '$':
        val, err := rr.readBulk()
        if err != nil {
            return nil, newError(err, "read bulk failed")
        }
        rp.vals = [][]byte{val}
        break
    case '*':
        rp.vals, err = rr.readMultiVals()
        if err != nil {
            return nil, newError(err, "read multi failed")
        }
        break
    default:
        return nil, newError(nil, "Unknown first byte of response: '%c'", rp.code)
    }

    return rp, nil
}

func (rr *Reader) ReadCommand() (cmd *Command, err os.Error) {
    firstChar, err := rr.rd.ReadByte()
    if err != nil {
        return nil, newError(err, "error reading first char from command")
    }

    // Gotta unread one byte
    err = rr.rd.UnreadByte()
    if err != nil {
        return nil, newError(err, "error unreading byte")
    }

    if firstChar == '*' {
        cmd, err = rr.readMultiCmd()
        if err != nil {
            return nil, newError(err, "readMulti failed")
        }
    } else {
        cmd, err = rr.readSingleCmd()
        if err != nil {
            return nil, newError(err, "readSingle failed")
        }
    }

    fmt.Printf("Got command %v!\n", cmd.Name)
    return cmd, nil
}

// TODO better error handling
func (rr *Reader) readBulk() (buff []byte, err os.Error) {
    // read the expected $
    b, err := rr.rd.ReadByte()
    if b != '$' || err != nil {
        return nil, newError(err, "bulk parse failed")
    }

    line, err := rr.readLineString()
    if err != nil {
        return nil, newError(err, "bulk parse failed")
    }

    // If its the same string don't bother parsing it
    if line == "-1" {
        return nil, nil
    }

    numBytes, err := strconv.Atoi(line)
    if err != nil {
        return nil, newError(err, "bulk parse failed")
    }

    buff = make([]byte, numBytes)

    nn, err := rr.rd.Read(buff)

    if nn != len(buff) || err != nil {
        return nil, newError(err, "bulk parse failed")
    }

    //seek to the end of the line
    rr.rd.ReadByte()
    rr.rd.ReadByte()

    return buff, nil
}
// hasNils is if we're doing a reply, -1's are nils
func (rr *Reader) readMultiVals() (vals [][]byte, err os.Error) {
    line, err := rr.readLineString()
    if err != nil {
        return nil, newError(err, "error reading first line of multicommand")
    }

    if line[0] != '*' {
        return nil, newError(nil, "expected '*' for bul reply")
    }

    nargs, err := strconv.Atoi(line[1:])
    switch {
    case err != nil:
        return nil, newError(err, "format not expected")
    case nargs < 1:
        return nil, newError(err, "bad format thought we'd have more than %d args", nargs)
    }

    vals = make([][]byte, nargs)
    for i := 0; i < nargs; i++ {
        vals[i], err = rr.readBulk()
        if err != nil {
            return nil, newError(err, "bulk parse failed")
        }
    }

    return vals, nil
}

// Takes the stream after the * has been read
func (rr *Reader) readMultiCmd() (cmd *Command, err os.Error) {
    print("got multi \n")

    cmd = new(Command)
    cmd.vals, err = rr.readMultiVals()
    if err != nil {
        return nil, newError(err, "failed reading cmd")
    }

    cmd.Name = cmd.vals[0]
    cmd.Args = cmd.vals[1:]

    return cmd, nil
}

func (rr *Reader) readSingleLineVals() (vals [][]byte, err os.Error) {
    ln, err := rr.readLineBytes()
    if err != nil {
        return nil, newError(err, "error reading single line of values")
    }
    vals = bytes.Split(ln, []byte{' '}, 0)
    return vals, nil
}

func (rr *Reader) readSingleCmd() (cmd *Command, err os.Error) {
    vals, err := rr.readSingleLineVals()
    if err != nil {
        return nil, newError(err, "error reading single command")
    }

    cmd = new(Command)
    cmd.Name = bytes.ToLower(vals[0])
    cmd.Args = vals[1:]

    fmt.Printf("got cmd '%s\n'", cmd.Name)

    isBulk := RedisCmds[string(cmd.Name)]&REDIS_CMD_BULK == REDIS_CMD_BULK

    // if its a bulk cmd, the last arg is the number of bytes of the real last arg
    // So lets swap it out
    if isBulk {
        nbytes, err := strconv.Atoi(
            string(cmd.Args[len(cmd.Args)]))
        if err != nil {
            return nil, newError(err, "parsing bulk argument length failed")
        }

        bulkbuf := make([]byte, nbytes)
        rr.rd.Read(bulkbuf)
        cmd.Args[len(cmd.Args)] = bulkbuf
    }

    // The line should end with crlf so check the last char is \r
    return cmd, nil
}

func (rr *Reader) readLineString() (line string, err os.Error) {
    line, err = rr.rd.ReadString('\n')
    if err != nil {
        return "", err
    }
    line = strings.TrimRight(line, LineDelim)
    return line, nil
}

func (rr *Reader) readLineBytes() (line []byte, err os.Error) {
    line, err = rr.rd.ReadBytes('\n')
    if err != nil {
        return nil, err
    }
    line = bytes.TrimRight(line, LineDelim)
    fmt.Printf("Got like %s\n", line)
    return line, nil
}

func (rr *Reader) peekByte() (b byte, err os.Error) {
    b, err = rr.rd.ReadByte()
    if err != nil {
        return b, newError(err, "error parsing first byte of reply")
    }
    err = rr.rd.UnreadByte()
    if err != nil {
        return 0, newError(err, "error unreading byte")
    }
    return b, nil
}


/*
 * redisio.writer
 */
type Writer struct {
    wr *bufio.Writer
}

func NewWriter(wr io.Writer) (rr *Writer) {
    rr = new(Writer)
    rr.wr = bufio.NewWriter(wr)
    return rr
}


func (rr *Writer) Flush() (err os.Error) {
    return rr.wr.Flush()
}


func (rr *Writer) WriteCommand(cmd *Command) (err os.Error) {

    err = rr.writeMultiBulk(cmd.vals)
    rr.Flush() //TODO add logic to only flush if there's no data left in buffer

    return nil
}

func (rr *Writer) WriteReply(rp *Reply) (err os.Error) {
    switch rp.code {
    case '*':
        return rr.writeMultiBulk(rp.vals)
    case '$':
        return rr.writeBulk(rp.vals[0])
    case '+', '-', ':':
        err = rr.wr.WriteByte(rp.code)
        if err != nil {
            return err
        }

        _, err = rr.wr.Write(rp.vals[0])
        if err != nil {
            return err
        }

        _, err = rr.wr.WriteString(LineDelim)
        if err != nil {
            return err
        }
        break
    default:
        return newError(nil, "unknown reply code")
    }
    return nil
}

func (rr *Writer) writeMultiBulk(vals [][]byte) (err os.Error) {
    nargs := len(vals) // ARgs  + cmd name

    err = rr.wr.WriteByte('*')
    if err != nil {
        return err
    }

    _, err = rr.wr.WriteString(strconv.Itoa(nargs) + LineDelim)
    if err != nil {
        return err
    }

    for _, val := range vals {
        err = rr.writeBulk(val)
        if err != nil {
            return err
        }
    }

    return nil
}

func (rr *Writer) writeBulk(arg []byte) (err os.Error) {
    err = rr.wr.WriteByte('$')
    if err != nil {
        return err
    }

    if arg == nil {
        _, err = rr.wr.WriteString("-1")
        if err != nil {
            return err
        }
    } else {
        _, err = rr.wr.WriteString(strconv.Itoa(len(arg)))
        if err != nil {
            return err
        }
        _, err = rr.wr.WriteString(LineDelim)
        if err != nil {
            return err
        }
        _, err = rr.wr.Write(arg)
        if err != nil {
            return err
        }
    }
    _, err = rr.wr.WriteString(LineDelim)
    if err != nil {
        return err
    }

    return nil
}
