package graph

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/emicklei/dot"
)

type Connection struct {
	Start GraphRef `json:"start"`
	End   GraphRef `json:"end"`
}

type Graph struct {
	Name        string       `json:"name"`
	Root        *Node        `json:"-"`
	Nodes       []Node       `json:"nodes"`
	Connections []Connection `json:"connections"`
}

func LoadFile(fn string) (*Graph, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	return Load(f)
}

func NewGraph(name string) *Graph {
	return &Graph{
		Name:        name,
		Root:        nil,
		Nodes:       []Node{},
		Connections: []Connection{},
	}
}

func Load(r io.Reader) (*Graph, error) {
	dec := json.NewDecoder(r)
	top := struct {
		Graph Graph `json:"graph"`
	}{}

	err := dec.Decode(&top)
	if err != nil {
		return nil, err
	}

	// We should have a single field, named graph.. with "nodes", and "connections"

	graph := &top.Graph
	graph.Root = &graph.Nodes[0]

	return graph, nil
}

func (g *Graph) AddNode(n Node) (*Node, error) {
	n.ID = uint(len(g.Nodes))
	g.Nodes = append(g.Nodes, n)
	if g.Root == nil {
		g.Root = &g.Nodes[n.ID]
	}
	return &g.Nodes[n.ID], nil
}

func (g *Graph) Connect(start NodeCollection, output uint, end NodeCollection, input uint) error {
	for _, n1 := range start.Nodes() {
		for _, n2 := range end.Nodes() {
			c := Connection{
				Start: NewOutputGraphRef(n1.ID, output),
				End:   NewInputGraphRef(n2.ID, input),
			}
			g.Connections = append(g.Connections, c)
		}
	}
	return nil
}

func (g *Graph) ConnectionsFrom(start GraphRef) []*Connection {
	var connects []*Connection
	for i := 0; i < len(g.Connections); i++ {
		if g.Connections[i].Start == start {
			connects = append(connects, &g.Connections[i])
		}
	}
	return connects
}

func (g *Graph) LeafNodes() []GraphRef {
	var leaves []GraphRef
	for _, node := range g.Nodes {
		total := 0
		for _, o := range node.Outputs {
			r := NewOutputGraphRef(node.ID, o.ID)
			total += len(g.ConnectionsFrom(r))
		}
		if total == 0 {
			r, _ := node.ToGraphRef()
			leaves = append(leaves, r)
		}
	}
	return leaves
}

var ErrorNodeNotFound = errors.New("Node not found in graph")

func (g *Graph) NodeByRef(ref GraphRef) (*Node, error) {
	if node_id, err := ref.NodeId(); err != nil {
		return nil, err
	} else if node_id > uint(len(g.Nodes)) {
		return nil, ErrorNodeNotFound
	} else {
		return &g.Nodes[node_id], nil
	}
}

func (g *Graph) NodeById(id uint) (*Node, error) {
	if id > uint(len(g.Nodes)) {
		return nil, ErrorNodeNotFound
	} else {
		return &g.Nodes[id], nil
	}
}

func (g *Graph) NodeByName(name string) (*Node, error) {
	for _, n := range g.Nodes {
		if n.Name == name {
			return &g.Nodes[n.ID], nil
		}
	}
	return nil, nil
}

func (g *Graph) WriteDot(w io.Writer) error {
	dg := dot.NewGraph(dot.Directed)
	for _, n := range g.Nodes {
		nodeID := fmt.Sprintf("%s_%d", n.Name, n.ID)
		dg.Node(nodeID)
	}
	for _, e := range g.Connections {
		n1, err := g.NodeByRef(e.Start)
		if err != nil {
			return err
		}
		n2, err := g.NodeByRef(e.End)
		if err != nil {
			return err
		}
		startID := fmt.Sprintf("%s_%d", n1.Name, n1.ID)
		endID := fmt.Sprintf("%s_%d", n2.Name, n2.ID)
		dg.Edge(dg.Node(startID), dg.Node(endID))
	}

	_, err := w.Write([]byte(dg.String()))
	return err
}
