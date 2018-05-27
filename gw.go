package gw

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/gorilla/websocket"
	"io"
	"net"
	"net/http"
	"strings"
)

type ErrorHandler func(error)
type ConnectHandler func(*NatsConn, *websocket.Conn) error

type NatsServerInfo string

type Settings struct {
	NatsAddr       string
	EnableTls      bool
	TlsConfig      *tls.Config
	ConnectHandler ConnectHandler
	ErrorHandler   ErrorHandler
	WSUpgrader     *websocket.Upgrader
	Trace          bool
	Filter         string
}

type Gateway struct {
	settings      Settings
	onError       ErrorHandler
	handleConnect ConnectHandler
}

var defaultUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type NatsConn struct {
	Conn       net.Conn
	CmdReader  CommandsReader
	ServerInfo NatsServerInfo
}

func (gw *Gateway) defaultConnectHandler(natsConn *NatsConn, wsConn *websocket.Conn) error {
	// Default behavior is to let the client on the other side do the CONNECT
	// after having forwarded the 'INFO' command
	infoCmd := append([]byte("INFO "), []byte(natsConn.ServerInfo)...)
	infoCmd = append(infoCmd, byte('\r'), byte('\n'))
	if gw.settings.Trace {
		fmt.Println("[TRACE] <--", string(infoCmd))
	}
	if err := wsConn.WriteMessage(websocket.TextMessage, infoCmd); err != nil {
		return err
	}
	return nil
}

func defaultErrorHandler(err error) {
	fmt.Println("[ERROR]", err)
}

//func copyAndTrace(prefix string, dst io.Writer, src io.Reader, buf []byte) (int64, error) {
//	read, err := src.Read(buf)
//	if err != nil {
//		return 0, err
//	}
//	fmt.Println("[TRACE]", prefix, string(buf[:read]))
//	written, err := dst.Write(buf[:read])
//	if written != read {
//		return int64(written), io.ErrShortWrite
//	}
//	return int64(written), err
//}

// Check subject against -filter
func checkSubject(message, filter string) bool {
	parts := strings.Split(message, " ")
	if len(parts) < 2 {
		return true // no subject - skip
	}
	subject := parts[1]
	farr := strings.Split(filter, ".")
	sarr := strings.Split(subject, ".")

	if len(sarr) < len(farr) {
		return false
	}

	for i, f := range farr {
		if sarr[i] != f && f != ">" && f != "*" {
			return false
		}
	}
	return true
}

// Filter messages to accepted and rejected
func filterMessages(messages []string, settings Settings) (accepted []string, rejected []string) {
	for _, message := range messages {
		if message == "" {
			continue
		}

		cmd := strings.ToUpper(substr(message, 0, 4))
		if cmd != "SUB " && cmd != "PUB " { // allow commands other than PUB/SUB
			accepted = append(accepted, message)
			continue
		}

		allowed := checkSubject(message, settings.Filter)
		if allowed { // allow allowed subjects
			accepted = append(accepted, message)
			continue
		}

		rejected = append(rejected, message)
	}
	return
}

// Send permission violation for message to WS
func wsPermissionViolation(message string, ws *websocket.Conn, trace bool) error {
	parts := strings.Split(message, " ")

	cmd := parts[0]
	switch cmd {
	case "SUB ":
		cmd = "Subscription"
	case "PUB ":
		cmd = "Publish"
	}

	subject := "?????"
	if len(parts) > 1 {
		subject = parts[1]
	}

	msg := fmt.Sprintf("-ERR 'Permissions Violation for %s to %s'\r\n", cmd, subject)
	if trace {
		fmt.Println("[TRACE]", "<--", msg)
	}
	if err := ws.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		return err
	}
	return nil
}

// Copy from WebSocket to NATS with applying -filter
func copyWsToNats(dst io.Writer, src io.Reader, buf []byte, ws *websocket.Conn, settings Settings) error {
	read, err := src.Read(buf)
	if err != nil {
		return err
	}
	msgStr := string(buf[:read])
	if settings.Trace {
		fmt.Println("[TRACE]", "-->", msgStr)
	}

	if settings.Filter != "" { // subjects filtering enabled
		accepted, rejected := filterMessages(strings.Split(msgStr, "\r\n"), settings)

		for _, message := range rejected { // return error to WS for each rejected message
			if err := wsPermissionViolation(message, ws, settings.Trace); err != nil {
				return err
			}
		}

		buf = []byte(strings.Join(accepted, "\r\n")) // update buf with merged accepted messages
		if len(buf) == 0 {
			return nil // nothing to send to NATS
		}
		buf = append(buf, 13, 10)
		read = len(buf)
	}

	written, err := dst.Write(buf[:read])
	if written != read {
		return io.ErrShortWrite
	}
	return err
}

func NewGateway(settings Settings) *Gateway {
	gw := Gateway{
		settings: settings,
	}
	gw.setErrorHandler(settings.ErrorHandler)
	gw.setConnectHandler(settings.ConnectHandler)
	return &gw
}

func (gw *Gateway) setErrorHandler(handler ErrorHandler) {
	if handler == nil {
		gw.onError = defaultErrorHandler
	} else {
		gw.onError = handler
	}
}

func (gw *Gateway) setConnectHandler(handler ConnectHandler) {
	if handler == nil {
		gw.handleConnect = gw.defaultConnectHandler
	} else {
		gw.handleConnect = handler
	}
}

func (gw *Gateway) natsToWsWorker(ws *websocket.Conn, src CommandsReader, doneCh chan<- bool) {
	defer func() {
		doneCh <- true
	}()

	for {
		cmd, err := src.nextCommand()
		if err != nil {
			gw.onError(err)
			return
		}
		if gw.settings.Trace {
			fmt.Println("[TRACE] <--", string(cmd))
		}
		if err := ws.WriteMessage(websocket.TextMessage, cmd); err != nil {
			gw.onError(err)
			return
		}
	}
}

func (gw *Gateway) wsToNatsWorker(nats net.Conn, ws *websocket.Conn, doneCh chan<- bool) {
	defer func() {
		doneCh <- true
	}()
	var buf []byte
	if gw.settings.Trace {
		buf = make([]byte, 1024*1024)
	}
	for {
		_, src, err := ws.NextReader()
		if err != nil {
			gw.onError(err)
			return
		}

		err = copyWsToNats(nats, src, buf, ws, gw.settings)

		//if gw.settings.Trace {
		//	_, err = copyAndTrace("-->", nats, src, buf)
		//} else {
		//	_, err = io.Copy(nats, src)
		//}
		if err != nil {
			gw.onError(err)
			return
		}
	}
}

func (gw *Gateway) Handler(w http.ResponseWriter, r *http.Request) {
	upgrader := defaultUpgrader
	if gw.settings.WSUpgrader != nil {
		upgrader = *gw.settings.WSUpgrader
	}
	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		gw.onError(err)
		return
	}
	natsConn, err := gw.initNatsConnectionForWSConn(wsConn)
	if err != nil {
		gw.onError(err)
		return
	}

	doneCh := make(chan bool)

	go gw.natsToWsWorker(wsConn, natsConn.CmdReader, doneCh)
	go gw.wsToNatsWorker(natsConn.Conn, wsConn, doneCh)

	<-doneCh

	wsConn.Close()
	natsConn.Conn.Close()

	<-doneCh
}

func readInfo(cmd []byte) (NatsServerInfo, error) {
	if !bytes.Equal(cmd[:5], []byte("INFO ")) {
		return "", fmt.Errorf("Invalid 'INFO' command: %s", string(cmd))
	}
	return NatsServerInfo(cmd[5 : len(cmd)-2]), nil
}

// initNatsConnectionForRequest open a connection to the nats server, consume the
// INFO message if needed, and finally handle the CONNECT
func (gw *Gateway) initNatsConnectionForWSConn(wsConn *websocket.Conn) (*NatsConn, error) {
	conn, err := net.Dial("tcp", gw.settings.NatsAddr)
	if err != nil {
		return nil, err
	}
	natsConn := NatsConn{Conn: conn, CmdReader: NewCommandsReader(conn)}

	// read the INFO, keep it
	infoCmd, err := natsConn.CmdReader.nextCommand()
	if err != nil {
		return nil, err
	}

	info, err := readInfo(infoCmd)

	if err != nil {
		return nil, err
	}

	natsConn.ServerInfo = info

	// optionnaly initialize the TLS layer
	// TODO check if the server requires TLS, which overrides the 'enableTls' setting
	if gw.settings.EnableTls {
		tlsConfig := gw.settings.TlsConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}
		tlsConn := tls.Client(conn, tlsConfig)
		tlsConn.Handshake()
		natsConn.Conn = tlsConn
		natsConn.CmdReader = NewCommandsReader(tlsConn)
	}

	if err := gw.handleConnect(&natsConn, wsConn); err != nil {
		return nil, err
	}

	return &natsConn, nil
}

func substr(s string, pos, length int) string {
	buf := []byte(s)
	l := pos + length
	if l > len(buf) {
		l = len(buf)
	}
	return string(buf[pos:l])
}
