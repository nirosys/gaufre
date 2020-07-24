package gaufre

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/nirosys/gaufre/data"
	"github.com/nirosys/gaufre/graph"
	"github.com/nirosys/gaufre/transport"
	"github.com/nirosys/gaufre/transport/local"

	log "github.com/sirupsen/logrus"

	"github.com/rs/xid"
)

var ErrorDuplicateNode error = errors.New("node type already registered")
var ErrorInvalidNodeType error = errors.New("invalid node type")

type GaufreConfig struct {
	NumWorkers  uint
	Transport   string
	FlowFactory data.FlowFunc
}

var DefaultConfig = &GaufreConfig{
	NumWorkers:  5,
	Transport:   "local",
	FlowFactory: flowFactoryPanic,
}

func flowFactoryPanic(context.Context, string) data.Flow {
	panic("Flow Factory not set")
}

// Runtime handles building the work queue and managing the Go routines used
// for processing flow packets.
type Runtime struct {
	nodeTypes  map[string]NodeImplementation
	nodeImpls  map[uint]NodeImplementation
	graph      *graph.Graph
	config     *GaufreConfig
	workers_wg sync.WaitGroup
	transport  transport.Transport
	output     chan interface{}
	mux        sync.Mutex
	metrics    *RuntimeMetrics

	leafNodes []uint                   // GraphRefs to each leaf node
	flowEnds  map[string]map[uint]bool // Flows that have partially finished, with each exited leaf.
}

// Register a custom node type.
func (r *Runtime) RegisterNodeType(name string, impl NodeImplementation) error {
	nameKey := strings.ToUpper(name)
	if _, exists := r.nodeTypes[nameKey]; exists {
		return ErrorDuplicateNode
	}
	r.nodeTypes[nameKey] = impl
	return nil
}

func (r *Runtime) GetNodeType(name string) (NodeImplementation, error) {
	nameKey := strings.ToUpper(name)
	if n, ok := r.nodeTypes[nameKey]; ok {
		return n, nil
	} else {
		return nil, ErrorInvalidNodeType
	}
}

func (r *Runtime) SetGraph(g *graph.Graph) {
	leaves := g.LeafNodes()
	leave_ids := make([]uint, len(leaves), len(leaves))
	for i, leaf := range leaves {
		if id, err := leaf.NodeId(); err != nil {
			fmt.Printf("Error getting node ID (%s): %s\n", leaf, err.Error())
		} else {
			leave_ids[i] = id
		}
	}
	r.leafNodes = leave_ids
	r.graph = g
}

func (r *Runtime) GetNodeImpl(id uint) (NodeImplementation, error) {
	// Hate this.. a lot.
	r.mux.Lock()
	defer r.mux.Unlock()
	if impl, ok := r.nodeImpls[id]; ok {
		return impl, nil
	} else {
		node, err := r.graph.NodeById(id)
		if err != nil {
			return nil, err
		}
		impl, err := r.newNodeImpl(node.Type)
		if err != nil {
			return nil, err
		}
		impl.Configure(&node.Configuration)
		r.nodeImpls[id] = impl
		return impl, nil
	}
}

// Instantiates a new instance of the NodeImplementation registered with the provided name.
func (r *Runtime) newNodeImpl(name string) (NodeImplementation, error) {
	n, ok := r.nodeTypes[strings.ToUpper(name)]
	if !ok {
		return nil, ErrorInvalidNodeType
	}

	impl := reflect.New(reflect.TypeOf(n).Elem()).Interface().(NodeImplementation)

	return impl, nil
}

func (r *Runtime) LoadFile(filename string) (*graph.Graph, error) {
	if g, err := graph.LoadFile(filename); err != nil {
		return nil, err
	} else {
		r.SetGraph(g)
		return g, nil
	}
}

func (r *Runtime) Load(reader io.Reader) (*graph.Graph, error) {
	if g, err := graph.Load(reader); err != nil {
		return nil, err
	} else {
		r.SetGraph(g)
		return g, nil
	}
}

func (r *Runtime) Shutdown() error {
	log := log.WithField("op", "gaufre:runtime.shutdown")
	log.Info("shutting down transport")
	r.transport.Close()
	log.Info("waiting for workers to finish")
	r.workers_wg.Wait()
	log.Info("closing output")
	close(r.output)
	return nil
}

func (r *Runtime) markLeafDone(ip *data.InformationPacket, node *graph.Node) error {
	r.mux.Lock()
	defer r.mux.Unlock()
	var nodes map[uint]bool
	has := false
	flowid := ip.Flow.ID()
	nodes, has = r.flowEnds[flowid]

	if !has {
		return errors.New("cannot mark unknown flow")
	}
	nodes[node.ID] = true

	done := true
	for _, fin := range nodes {
		done = done && fin
	}

	if done {
		ip.Flow.Close()
		r.metrics.FlowEnd(flowid)
		delete(r.flowEnds, flowid)
	}

	return nil
}

func (r *Runtime) worker() {
	defer func() { r.workers_wg.Done() }()

	log := log.WithField("op", "gaufre:runtime.worker")

	// TODO: Allow workers to shutdown without killing the transport, since
	//       they need the transport to return data.

	done := false
	for !done {
		p, err := r.transport.Receive(context.TODO())
		if errors.Is(err, transport.ErrTransportClosed) {
			done = true
			continue
		} else if err != nil {
			log.WithField("err", err.Error()).Error("error occurred receiving info packet")
			continue
		}
		// We cannot reference multiple graphs yet, so we need to default to
		// the only graph in the runtime.
		nid, err := p.Connection.End.NodeId()
		if err != nil {
			log.Info("Error: node does not exist for received packet")
			continue
		}
		node, err := r.graph.NodeById(nid)
		if err != nil {
			log.WithField("err", err.Error()).WithField("node_id", nid).Error("error getting node by id")
			continue
		}
		if nodeType, err := r.GetNodeImpl(node.ID); err != nil {
			log.WithField("err", err.Error()).WithField("node_id", nid).Error("error getting node implementation")
			continue
		} else {
			flowid := p.Flow.ID()
			log = log.WithField("flowid", flowid)
			ret_ip, err := nodeType.HandleInfoPacket(r, node, p)

			// If we have an error, we need to let the flow know there has been an
			// error, and propagate the error through the rest of the graph. This
			// is super important because an error resulting in the end of a flow
			// will not trigger the end of the flow unless the downstream nodes see
			// the PacketTypeEndFlow flag.
			if err != nil {
				p.Flow.Error(err)
				ptype := data.PacketTypeError
				if p.Type&data.PacketTypeEndFlow != 0 {
					ptype |= data.PacketTypeEndFlow
				}
				// After logging, we're going to pass the error down as an error packet
				// So we need to get all of the outputs from this node.. currently,
				// we only have 1 output in a graph (for snitch) so this needs to be
				// fixed soon.
				outputRef, err := node.OutputRefByName("output")
				if err != nil {
					log.WithField("err", err.Error()).WithField("node_id", nid).Error("error getting output ref")
					continue
				}

				ret_ip = &data.InformationPacket{
					Type:       ptype,
					Flow:       p.Flow,
					Connection: graph.Connection{Start: outputRef},
					Priority:   0,
				}
			}
			if ret_ip != nil {
				// If we've received a NOP packet, there's nothing for us to do as
				// the node did not emit anything. We do not signal that to the downstream
				// nodes.
				if ret_ip.Type&data.PacketTypeNop != 0 {
					continue
				}

				connects := r.graph.ConnectionsFrom(ret_ip.Connection.Start)
				ret_ip.Flow = p.Flow

				// If we are an end flow packet, mark the flow finished.
				// We could end a flow that was entirely in error, with no data.
				if ret_ip.Type&data.PacketTypeEndFlow != 0 && len(connects) == 0 {
					r.markLeafDone(p, node)
				}

				// If we have data, but do not have any output connections, then we
				// must be looking at leaf nodes and can send our data to the ouput
				// channel so the hosting app can do what it needs to. TODO Eventually,
				// this shouldn't happen, the hosting app should implement node types
				// that handle this and connect all leaf nodes to it.
				if (ret_ip.Type&data.PacketTypeData != 0) && (len(connects) == 0) {
				LOOP:
					for {
						select {
						case r.output <- ret_ip.Data:
							break LOOP
						case <-time.After(1 * time.Second):
							log.WithField("node", ret_ip.Connection.Start).Warn("waited over 1 second to return data")
						}
					}
				} else {
					// We make copies of the IP and send them to the downstream nodes.
					// This is fairly light weight, as what is being copied is just the
					// type, priority, connection, and a couple pointers.
					for _, c := range connects {
						newPkt := *ret_ip
						newPkt.Connection.End = c.End
						if err := r.transport.Send(context.TODO(), &newPkt); err != nil {
							log.WithField("error", err.Error()).Error("error sending info packet using transport")
						}
					}
				}
			}
		}
	}
}

func (r *Runtime) init() error {
	log := log.WithField("op", "gaufre:runtime.init")

	log.WithField("workers", r.config.NumWorkers).Info("initializing runtime")

	switch strings.ToLower(r.config.Transport) {
	case "local":
		if t, err := local.NewLocalTransport(); err != nil {
			return err
		} else {
			r.transport = t
		}
	}
	r.output = make(chan interface{}, 10)

	r.workers_wg.Add(int(r.config.NumWorkers))
	for i := uint(0); i < r.config.NumWorkers; i++ {
		go r.worker()
	}

	return nil
}

func (r *Runtime) Send(ctx context.Context, d interface{}) error {
	if r.graph == nil {
		return errors.New("no graph")
	}
	g := r.graph
	inputRef := graph.NewInputGraphRef(g.Root.ID, g.Root.Inputs.ID)

	flowid := xid.New().String()
	flow := r.config.FlowFactory(ctx, flowid)

	r.mux.Lock()
	nodes := make(map[uint]bool)
	for _, node_id := range r.leafNodes {
		nodes[node_id] = false
	}
	r.flowEnds[flowid] = nodes
	r.mux.Unlock()

	r.metrics.FlowBegin(flowid)

	ip := &data.InformationPacket{
		Connection: graph.Connection{Start: "$ref:/", End: inputRef},
		Data:       d,
		Type:       data.PacketTypeData | data.PacketTypeEndFlow,
		Flow:       flow,
	}

	return r.transport.Send(ctx, ip)
}

func (r *Runtime) Output() <-chan interface{} {
	return r.output
}

func (r *Runtime) Validate(g *graph.Graph) error {
	for _, n := range g.Nodes {
		if _, err := r.GetNodeType(n.Type); err != nil {
			return err
		}
	}
	// TODO: Sanity checks:
	//   * Ensure no disconnected nodes
	//   * Ensure all nodes have an input connection
	return nil
}

func (r *Runtime) EmitMetrics() {
	rtMetrics := r.metrics.Metrics()
	logger := log.WithField("op", "gaufre:metrics")

	logger.WithFields(log.Fields(rtMetrics)).Info("runtime metrics")

	tMetrics := r.transport.Metrics()
	logger.WithFields(log.Fields(tMetrics)).Info("transport metrics")
}

// Create a new Runtime that can be used for executing DAGs.
func NewRuntime(cfg *GaufreConfig) (*Runtime, error) {
	rt := &Runtime{
		nodeTypes: map[string]NodeImplementation{},
		nodeImpls: map[uint]NodeImplementation{},
		metrics:   NewRuntimeMetrics(),
		config:    cfg,
		flowEnds:  make(map[string]map[uint]bool),
	}

	if err := rt.init(); err != nil {
		return nil, err
	}

	return rt, nil
}
