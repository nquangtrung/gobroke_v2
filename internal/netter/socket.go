package netter

import (
	"fmt"
	"net"
)

type ConnectionType int

const (
	UNIX ConnectionType = iota
	TCP
)

func CreateServerSocket(t ConnectionType) (net.Listener, error) {
	switch t {
	case UNIX:
		socket, err := net.Listen("unix", "/tmp/gobroke.sock")
		if err != nil {
			return nil, err
		}
		return socket, nil
	case TCP:
		socket, err := net.Listen("tcp", "localhost:8080")
		if err != nil {
			return nil, err
		}
		return socket, nil
	default:
		return nil, fmt.Errorf("Unsupported connection type")
	}
}
func CreateClientConnection(t ConnectionType) (net.Conn, error) {
	switch t {
	case UNIX:
		socket, err := net.Dial("unix", "/tmp/gobroke.sock")
		if err != nil {
			return nil, err
		}
		return socket, nil
	case TCP:
		socket, err := net.Dial("tcp", "localhost:8080")
		if err != nil {
			return nil, err
		}
		return socket, nil
	default:
		return nil, fmt.Errorf("Unsupported connection type")
	}
}
