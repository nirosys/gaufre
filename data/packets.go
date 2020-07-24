package data

import (
	"context"

	"github.com/nirosys/gaufre/graph"
)

type Flow interface {
	Context() context.Context
	ID() string
	Close() error
	Error(error)
}

type FlowFunc func(context.Context, string) Flow

type PacketType uint

const (
	PacketTypeShutdown PacketType = 0x01 // Shutdown the DAG
	PacketTypeData     PacketType = 0x02 // Packet has data attached
	PacketTypeEndFlow  PacketType = 0x04 // Packet is the last packet, from this node, for the flow
	PacketTypeError    PacketType = 0x08 // Packet contains an error
	PacketTypeNop      PacketType = 0x10 // Packet does not need to be sent to downstream nodes; No action required
)

// An InformationPacket is the data that is sent between two nodes in the DAG
// during execution. By using a wrapper like this, we're able to do out of band
// communications between nodes for non-data events like signaling the end of a
// data stream.
type InformationPacket struct {
	Connection graph.Connection
	Data       interface{}
	Flow       Flow

	Type     PacketType
	Priority int // Used during runtime to prioritize work items
}
