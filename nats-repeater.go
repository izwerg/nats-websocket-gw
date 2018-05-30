package gw

import (
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"strconv"
	"strings"
)

// Authorize PUB/SUB subject against -filter
func (c *client) authorize(cmd, subject string) bool {
	if subject == "" || c.filter == "" {
		return true // no subject - ok, no filter - ok
	}
	filtArr := strings.Split(c.filter, ".")
	subjArr := strings.Split(subject, ".")

	if len(subjArr) < len(filtArr) {
		return false // subject cannot be shorter than filter
	}

	for i, f := range filtArr {
		if subjArr[i] != f && f != ">" && f != "*" {
			return false // mismatch without wildcard
		}
	}
	return true
}

func (c *client) processPub(arg []byte) error {
	argArr := strings.Split(string(arg), " ")
	if len(argArr) < 2 {
		return errors.New("Invalid number of arguments for PUB")
	}
	var err error
	if c.pa.size, err = strconv.Atoi(argArr[1]); err != nil {
		return err
	}

	err = c.sendErr(fmt.Sprintf("Permissions Violation for Publish to %s", argArr[0]))
	return err
}

func (c *client) processMsg(msgBuf []byte) {
	// ignore PUB payload because we don't pass PUB to NATS yet
}

func (c *client) processSub(arg []byte) error {
	subject := strings.Split(string(arg), " ")[0]

	if c.authorize("SUB", subject) {
		buf := []byte("SUB ")
		buf = append(buf, arg...)
		buf = append(buf, []byte(CR_LF)...)
		err := c.writeNats(buf)
		return err
	}
	err := c.sendErr(fmt.Sprintf("Permissions Violation for Subscription to %s", subject))
	return err
}

func (c *client) processUnsub(arg []byte) error {
	buf := []byte("UNSUB ")
	buf = append(buf, arg...)
	buf = append(buf, []byte(CR_LF)...)
	err := c.writeNats(buf)
	return err
	return nil
}

func (c *client) processPing() error {
	buf := []byte("PING")
	buf = append(buf, []byte(CR_LF)...)
	err := c.writeNats(buf)
	return err
}

func (c *client) processPong() error {
	buf := []byte("PONG")
	buf = append(buf, []byte(CR_LF)...)
	err := c.writeNats(buf)
	return err
}

func (c *client) processConnect(arg []byte) error {
	buf := []byte("CONNECT ")
	buf = append(buf, arg...)
	buf = append(buf, []byte(CR_LF)...)
	err := c.writeNats(buf)
	return err
}

func (c *client) writeNats(buf []byte) error {
	if c.trace {
		fmt.Println("[TRACE]", "-->", string(buf))
	}
	_, err := c.dst.Write(buf)
	return err
}

func (c *client) sendErr(text string) error {
	msg := fmt.Sprintf("-ERR '%s'\r\n", text)
	if c.trace {
		fmt.Println("[TRACE]", "<--", msg)
	}
	err := c.ws.WriteMessage(websocket.TextMessage, []byte(msg))
	return err
}
