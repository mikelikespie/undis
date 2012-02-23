package redisio

import (
    "bufio"
    "bytes"
    "fmt"
    "io"
    "strconv"
    "strings"
)

const (
    Test      = 3
    LineDelim = "\r\n"
)

type RedisIOError struct {
    Msg string
    Err error
}

func (e *RedisIOError) Error() string {
    if e.Err == nil {
        return "redis protocol error " + e.Msg
    }

    return "redis protocol error caught: " + e.Msg + "\n\t[" + e.Err.Error() + "]"
}

// origError can be nil
func newError(origError error, msgFmt string, a ...interface{}) (err error) {
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
func (rr *Reader) ReadReply() (rp *Reply, err error) {
    rp = new(Reply)
    if rp.code, err = rr.peekByte(); err != nil {
        return nil, err
    }

    switch rp.code {
    case '-', '+', ':':
        if ln, err := rr.readLineBytes(); err != nil {
            return nil, newError(err, "error reading line bytes")
        } else {
            rp.vals = [][]byte{ln[1:]}
        }
        break
    case '$':
        if val, err := rr.readBulk(); err != nil {
            return nil, newError(err, "read bulk failed")
        } else {
            rp.vals = [][]byte{val}
        }
        break
    case '*':
        if rp.vals, err = rr.readMultiVals(); err != nil {
            return nil, newError(err, "read multi failed")
        }
        break
    default:
        return nil, newError(nil, "Unknown first byte of response: '%c'", rp.code)
    }

    return rp, nil
}

func (rr *Reader) ReadCommand() (cmd *Command, err error) {
    var firstChar byte

    if firstChar, err = rr.rd.ReadByte(); err != nil {
        return nil, newError(err, "error reading first char from command")
    }

    // Gotta unread one byte
    if err = rr.rd.UnreadByte(); err != nil {
        return nil, newError(err, "error unreading byte")
    }

    if firstChar == '*' {
        if cmd, err = rr.readMultiCmd(); err != nil {
            return nil, newError(err, "readMulti failed")
        }
    } else if cmd, err = rr.readSingleCmd(); err != nil {
        return nil, newError(err, "readSingle failed")
    }

    fmt.Printf("Got command %v!\n", cmd.Name)
    return cmd, nil
}

// TODO better error handling
func (rr *Reader) readBulk() (buff []byte, err error) {
    var line string

    if b, err := rr.rd.ReadByte(); b != '$' || err != nil {
        return nil, newError(err, "bulk parse failed")
    }

    if line, err = rr.readLineString(); err != nil {
        return nil, newError(err, "bulk parse failed")
    }

    // If its the same string don't bother parsing it
    if line == "-1" {
        return nil, nil
    }

    if numBytes, err := strconv.Atoi(line); err != nil {
        return nil, newError(err, "bulk parse failed")
    } else {
        buff = make([]byte, numBytes)
    }

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
func (rr *Reader) readMultiVals() (vals [][]byte, err error) {
    var line string

    if line, err = rr.readLineString(); err != nil {
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
        if vals[i], err = rr.readBulk(); err != nil {
            return nil, newError(err, "bulk parse failed")
        }
    }

    return vals, nil
}

// Takes the stream after the * has been read
func (rr *Reader) readMultiCmd() (cmd *Command, err error) {
    print("got multi \n")

    cmd = new(Command)
    if cmd.vals, err = rr.readMultiVals(); err != nil {
        return nil, newError(err, "failed reading cmd")
    }

    cmd.Name = cmd.vals[0]
    cmd.Args = cmd.vals[1:]

    return cmd, nil
}

func (rr *Reader) readSingleLineVals() (vals [][]byte, err error) {
    if ln, err := rr.readLineBytes(); err != nil {
        return nil, newError(err, "error reading single line of values")
    } else {
        vals = bytes.SplitN(ln, []byte{' '}, 0)
    }
    return vals, nil
}

func (rr *Reader) readSingleCmd() (cmd *Command, err error) {
    if vals, err := rr.readSingleLineVals(); err != nil {
        return nil, newError(err, "error reading single command")
    } else {
        cmd = new(Command)
        cmd.Name = bytes.ToLower(vals[0])
        cmd.Args = vals[1:]
    }

    fmt.Printf("got cmd '%s\n'", cmd.Name)

    isBulk := RedisCmds[string(cmd.Name)]&REDIS_CMD_BULK == REDIS_CMD_BULK

    // if its a bulk cmd, the last arg is the number of bytes of the real last arg
    // So lets swap it out
    if isBulk {
        if nbytes, err := strconv.Atoi(string(cmd.Args[len(cmd.Args)])); err != nil {
            return nil, newError(err, "parsing bulk argument length failed")
        } else {

            bulkbuf := make([]byte, nbytes)
            rr.rd.Read(bulkbuf)
            cmd.Args[len(cmd.Args)] = bulkbuf
        }
    }

    // The line should end with crlf so check the last char is \r
    return cmd, nil
}

func (rr *Reader) readLineString() (line string, err error) {
    if line, err = rr.rd.ReadString('\n'); err != nil {
        return "", err
    }
    line = strings.TrimRight(line, LineDelim)
    return line, nil
}

func (rr *Reader) readLineBytes() (line []byte, err error) {
    if line, err = rr.rd.ReadBytes('\n'); err != nil {
        return nil, err
    }
    line = bytes.TrimRight(line, LineDelim)
    fmt.Printf("Got like %s\n", line)
    return line, nil
}

func (rr *Reader) peekByte() (b byte, err error) {
    if b, err = rr.rd.ReadByte(); err != nil {
        return b, newError(err, "error parsing first byte of reply")
    }
    if err = rr.rd.UnreadByte(); err != nil {
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

func (rr *Writer) Flush() (err error) {
    return rr.wr.Flush()
}

func (rr *Writer) WriteCommand(cmd *Command) (err error) {

    err = rr.writeMultiBulk(cmd.vals)
    rr.Flush() //TODO add logic to only flush if there's no data left in buffer

    return nil
}

func (rr *Writer) WriteReply(rp *Reply) (err error) {
    switch rp.code {
    case '*':
        return rr.writeMultiBulk(rp.vals)
    case '$':
        return rr.writeBulk(rp.vals[0])
    case '+', '-', ':':
        if err = rr.wr.WriteByte(rp.code); err != nil {
            return err
        }

        if _, err = rr.wr.Write(rp.vals[0]); err != nil {
            return err
        }

        if _, err = rr.wr.WriteString(LineDelim); err != nil {
            return err
        }
        break
    default:
        return newError(nil, "unknown reply code")
    }
    return nil
}

func (rr *Writer) writeMultiBulk(vals [][]byte) (err error) {
    nargs := len(vals) // ARgs  + cmd name

    if err = rr.wr.WriteByte('*'); err != nil {
        return err
    }

    if _, err = rr.wr.WriteString(strconv.Itoa(nargs) + LineDelim); err != nil {
        return err
    }

    for _, val := range vals {
        if err = rr.writeBulk(val); err != nil {
            return err
        }
    }

    return nil
}

func (rr *Writer) writeBulk(arg []byte) (err error) {
    if err = rr.wr.WriteByte('$'); err != nil {
        return err
    }

    if arg == nil {
        if _, err = rr.wr.WriteString("-1"); err != nil {
            return err
        }
    } else {
        if _, err = rr.wr.WriteString(strconv.Itoa(len(arg))); err != nil {
            return err
        }
        if _, err = rr.wr.WriteString(LineDelim); err != nil {
            return err
        }
        if _, err = rr.wr.Write(arg); err != nil {
            return err
        }
    }
    if _, err = rr.wr.WriteString(LineDelim); err != nil {
        return err
    }

    return nil
}
