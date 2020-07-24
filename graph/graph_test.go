package graph_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/nirosys/gaufre/data"
	"github.com/nirosys/gaufre/graph"
)

type addNode struct{}

func (n *addNode) Configure(c *graph.NodeConfig) error {
	fmt.Printf("Configuring AddNode\n")
	return nil
}

func (n *addNode) Receive(ctx context.Context, graph *graph.Graph, data data.Data) error {
	return nil
}

// Test loading a graph via JSON representation
func TestGraphLoad(t *testing.T) {
	myGraph := `
{"graph": {
  "name": "Compute 5 + 10 (Operations)",
  "nodes": [
    {
		 "id": 0,
		 "name": "Emit 5",
		 "type": "emit5",
		 "outputs": [
			 {"id": 0, "name": "5"}
		 ]
	 },
	 {
	    "id": 1,
		 "name": "Add 10",
		 "type": "add",
		 "outputs": [
		    {"id": 0, "name": "finished"}
		 ]
	 }
  ],
  "connections": [
     {"start": "$ref:/node/0/output/0", "end": "$ref:/node/1/input/0"}
  ]
}}`
	g, err := graph.Load(strings.NewReader(myGraph))
	if err != nil {
		t.Error(err)
		return
	}

	if g.Name != "Compute 5 + 10 (Operations)" {
		t.Errorf("Unexpected name: '%s' != '%s'", g.Name, "Compute 5 + 10 (Operations)")
		return
	}

	if n := len(g.Nodes); n != 2 {
		t.Errorf("Unexpected number of nodes: %d != 2", n)
		return
	}

	if n := len(g.Connections); n != 1 {
		t.Errorf("Unexpected number of connections: %d != 1", n)
		return
	}

	if node, err := g.NodeByRef("$ref:/node/0"); err != nil {
		t.Errorf("Error getting node by ref: %s", err.Error())
		return
	} else if node == nil {
		t.Errorf("Node not found when searching by ref")
		return
	} else {
		if node.Name != "Emit 5" {
			t.Errorf("Unexpected name for node 0: '%s' != 'Emit 5'", node.Name)
			return
		}
	}
}
