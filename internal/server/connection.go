package server

import (
	"context"
	"net"
)

type Connection interface {
	ID() string
	Conn() net.Conn
	Loop(ctx context.Context, broker *Broker)
}
