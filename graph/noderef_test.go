package graph_test

import (
	"testing"

	"github.com/nirosys/gaufre/graph"
)

func TestGraphRefType(t *testing.T) {
	tests_values := []struct {
		Reference graph.GraphRef
		Type      graph.GraphRefType
	}{
		{Reference: "$ref:/node/0", Type: graph.GraphRefTypeNode},
		{Reference: "$ref:/node/0/input/0", Type: graph.GraphRefTypeInput},
		{Reference: "$ref:/node/0/output/0", Type: graph.GraphRefTypeOutput},
	}

	for i, test := range tests_values {
		if ref_type, err := test.Reference.Type(); err != nil {
			t.Errorf("[%d] Unexpected error: %s", i, err.Error())
		} else if ref_type != test.Type {
			if ref_type != graph.GraphRefTypeNode {
				t.Errorf("[%d] Unexpected reference type: %d != %d", i, ref_type, test.Type)
			}
		}
	}
}

func TestGraphRefComponents(t *testing.T) {
	tests_values := []struct {
		Reference graph.GraphRef
		Type      graph.GraphRefType
		NodeId    uint
		SocketId  uint
	}{
		{Reference: "$ref:/node/0", Type: graph.GraphRefTypeNode, NodeId: 0, SocketId: 0},
		{Reference: "$ref:/node/1", Type: graph.GraphRefTypeNode, NodeId: 1, SocketId: 0},
		{Reference: "$ref:/node/5", Type: graph.GraphRefTypeNode, NodeId: 5, SocketId: 0},
		{Reference: "$ref:/node/0/output/1", Type: graph.GraphRefTypeOutput, NodeId: 0, SocketId: 1},
		{Reference: "$ref:/node/1/input/2", Type: graph.GraphRefTypeInput, NodeId: 1, SocketId: 2},
	}

	for i, test := range tests_values {
		if node_id, err := test.Reference.NodeId(); err != nil {
			t.Errorf("[%d] Unexpected error: %s", i, err.Error())
		} else if node_id != test.NodeId {
			t.Errorf("[%d] Unexpected node ID: %d != %d", i, node_id, test.NodeId)
		}

		tpe, _ := test.Reference.Type() // err would present itself above.
		if tpe == graph.GraphRefTypeInput || tpe == graph.GraphRefTypeOutput {
			if sockid, err := test.Reference.SocketId(); err != nil {
				t.Errorf("[%d] Unexpected error: %s", i, err.Error())
			} else if sockid != test.SocketId {
				t.Errorf("[%d] Unexpected socket ID: %d != %d", i, sockid, test.SocketId)
			}
		}
	}
}
