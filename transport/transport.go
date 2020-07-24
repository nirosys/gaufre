package transport

import (
	"context"
	"errors"

	"github.com/nirosys/gaufre/data"
)

var ErrTransportClosed = errors.New("transport closed")

type Transport interface {
	// Sends an individual IP over the transport.
	Send(context.Context, *data.InformationPacket) error

	// Receives an individual IP from the transport.
	Receive(context.Context) (*data.InformationPacket, error)

	// Returns a channel the transport will use for communicating errors.
	Errors() <-chan error

	// Closes the transport, cleaning up any resources after processing all IPs
	Close()

	// Transports need to provide stats about runtime behaviors.
	data.MetricsProvider
}
