package gaufre

import (
	"github.com/nirosys/gaufre/data"
	"github.com/nirosys/gaufre/graph"
)

type NodeImplementation interface {
	Configure(*graph.NodeConfig) error
	HandleInfoPacket(*Runtime, *graph.Node, *data.InformationPacket) (*data.InformationPacket, error)
	//HandleInfoPacket(context.Context, *Runtime, *graph.Node, *data.InformationPacket) (*data.InformationPacket, error)
	//	ProcessData(context.Context, *Runtime, *data.Data) (*data.InformationPacket, error)
}
