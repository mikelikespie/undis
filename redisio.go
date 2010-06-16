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
    Test = 3
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

    return "redis protocol error caught " + e.Msg + ": " + e.Error.String()
}


// origError can be nil
func newError(origError os.Error, msgFmt string, a ...interface{}) (err os.Error) {
    return &RedisIOError{fmt.Sprintf(msgFmt, a), origError}
}


type RedisCommand struct {
    name []byte
    args [][]byte
}

func (rc *RedisCommand) String() string {
    return fmt.Sprintf("RedisCommand(name='%s', args={'%s'})",
        rc.name, bytes.Join(rc.args, []byte("', '")))
}


type Reader struct {
    rd *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
    rr := new(Reader)
    rr.rd = bufio.NewReader(rd)
    return rr
}

func (rr *Reader) ReadCommand() (cmd *RedisCommand, err os.Error) {
    firstChar, err := rr.rd.ReadByte()

    if err != nil {
        return nil, newError(err, "error reading first char from command")
    }

    if firstChar == '*' {
        cmd, err = rr.parseMulti()
        if err != nil {
            return nil, newError(err, "parseMulti failed")
        }
    } else {
        rr.rd.UnreadByte() // Gotta unread one byte
        cmd, err = rr.parseSingle()
        if err != nil {
            return nil, newError(err, "parseSingle failed")
        }
    }

    fmt.Printf("Got command %v!\n", cmd.name)
    return cmd, nil
}

// TODO better error handling
func (rr *Reader) parseBulk() (buff []byte, err os.Error) {
    // read the expected $
    b, err := rr.rd.ReadByte()
    if b != '$' || err != nil {
        return nil, newError(err, "bulk parse failed")
    }

    line, err := rr.rd.ReadString('\n')
    if err != nil {
        return nil, newError(err, "bulk parse failed")
    }

    line = strings.TrimRight(line, LineDelim)

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

// Takes the stream after the * has been read
func (rr *Reader) parseMulti() (cmd *RedisCommand, err os.Error) {
    print("Got multi \n")

    line, err := rr.rd.ReadString('\n')

    if err != nil {
        return nil, newError(err, "error reading first line of multicommand")
    }

    line = strings.TrimRight(line, LineDelim)

    nargs, err := strconv.Atoi(line)
    if err != nil {
        return nil, newError(err, "format not expected")
    } else if nargs < 1 {
        return nil, newError(err, "bad format thought we'd have more than %d args", nargs)
    }

    cmd = new(RedisCommand)

    bulk, err := rr.parseBulk()
    if err != nil {
        return nil, newError(err, "bulk parse failed")
    }

    cmd.name = bytes.ToLower(bulk)

    nargs-- // Subtract one off because we're going to be parsing the cmd Name
    cmd.args = make([][]byte, nargs)

    for i := 0; i < nargs; i++ {
        cmd.args[i], err = rr.parseBulk()
        if err != nil {
            return nil, newError(err, "bulk parse failed")
        }
    }

    return cmd, nil
}

func (rr *Reader) parseSingle() (cmd *RedisCommand, err os.Error) {
    cmd = new(RedisCommand)
    cmd.name, err = rr.rd.ReadBytes(' ')

    if err != nil {
        return nil, newError(err, "error reading single command")
    }
    cmd.name = bytes.ToLower(bytes.TrimRight(cmd.name, " "))

    fmt.Printf("Got cmd '%s\n'", cmd.name)

    line, err := rr.rd.ReadBytes('\n')
    if err != nil || line[len(line)-2] != '\r' {
        return nil, newError(err, "error reading single command")
    }
    line = bytes.TrimRight(line, LineDelim)

    cmd.args = bytes.Split(line, []byte{' '}, 0)

    isBulk := RedisCmds[string(cmd.name)]&REDIS_CMD_BULK == REDIS_CMD_BULK

    // if its a bulk cmd, the last arg is the number of bytes of the real last arg
    // So lets swap it out
    if isBulk {
        nbytes, err := strconv.Atoi(string(cmd.args[len(cmd.args)-1]))
        if err != nil {
            return nil, newError(err, "parsing bulk argument length failed")
        }

        bulkbuf := make([]byte, nbytes)
        rr.rd.Read(bulkbuf)
        cmd.args[len(cmd.args)-1] = bulkbuf
    }

    // The line should end with crlf so check the last char is \r
    return cmd, nil
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
	return rr.wr.Flush();
}


func (rr *Writer) WriteCommand(cmd *RedisCommand) (err os.Error) {
	rr.wr.WriteByte('*')
	nargs := 1 + len(cmd.args) // ARgs  + cmd name
	_, err = rr.wr.WriteString(strconv.Itoa(nargs))
	if err != nil { return err }

	_, err = rr.wr.WriteString(LineDelim)
	if err != nil { return err }

	rr.Flush() //TODO add logic to only flush if there's no data left in buffer

	return nil
}

func (rr *Writer) writeBulk(arg []byte) (err os.Error) {
	err = rr.wr.WriteByte('$')
	if err != nil { return err }

	_, err = rr.wr.WriteString(strconv.Itoa(len(arg)))
	if err != nil { return err }

	_, err = rr.wr.WriteString(LineDelim)
	if err != nil { return err }

	_, err = rr.wr.Write(arg)
	if err != nil { return err }

	_, err = rr.wr.WriteString(LineDelim)
	if err != nil { return err }

	return nil
}
