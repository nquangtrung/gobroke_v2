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

type ConnectionParams struct {
	Type       ConnectionType
	Address    string
	SocketPath string
}

func CreateServerSocket(params ConnectionParams) (net.Listener, error) {
	switch params.Type {
	case UNIX:
		socket, err := net.Listen("unix", params.SocketPath)
		if err != nil {
			return nil, err
		}
		return socket, nil
	case TCP:
		socket, err := net.Listen("tcp", params.Address)
		if err != nil {
			return nil, err
		}
		return socket, nil
	default:
		return nil, fmt.Errorf("Unsupported connection type")
	}
}
func CreateClientConnection(params ConnectionParams) (net.Conn, error) {
	switch params.Type {
	case UNIX:
		socket, err := net.Dial("unix", params.SocketPath)
		if err != nil {
			return nil, err
		}
		return socket, nil
	case TCP:
		socket, err := net.Dial("tcp", params.Address)
		if err != nil {
			return nil, err
		}
		return socket, nil
	default:
		return nil, fmt.Errorf("Unsupported connection type")
	}
}
