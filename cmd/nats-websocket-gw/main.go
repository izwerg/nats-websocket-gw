package main

import (
	"flag"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/izwerg/nats-websocket-gw"
	"net/http"
)

func main() {
	var noOriginCheck, trace bool
	var natsAddr, wsAddr, wsRoute, filter string

	flag.BoolVar(&noOriginCheck, "no-origin-check", false, "no origin check for WebSockets")
	flag.BoolVar(&trace, "trace", false, "enable tracing")
	flag.StringVar(&natsAddr, "nats-addr", "localhost:4222", "host:port of NATS server")
	flag.StringVar(&wsAddr, "ws-addr", "0.0.0.0:8910", "host:port for WebSockets")
	flag.StringVar(&wsRoute, "ws-route", "/nats", "route for WebSockets")
	flag.StringVar(&filter, "filter", "", "filter allowed NATS subjects")

	flag.Parse()

	settings := gw.Settings{
		NatsAddr: natsAddr,
		Trace:    trace,
		Filter:   filter,
	}

	if noOriginCheck {
		settings.WSUpgrader = &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		}
	}

	gateway := gw.NewGateway(settings)
	http.HandleFunc(wsRoute, gateway.Handler)
	fmt.Println(http.ListenAndServe(wsAddr, nil))
}
