package graph

import (
	"errors"
	"fmt"
)

var ErrorOutputNotFound = errors.New("Output not found")

// References:
//     $ref:/foo/input/bar
// Graph gets an ID.
//   Each node gets an ID.
//      Each input gets an ID
//      Each output gets an ID
//
//  $ref://gid/input/iid
//  $ref://gid/output/iid

type Output struct {
	ID   uint   `json:"id"`
	Name string `json:"name"`
}

type Input struct {
	ID   uint   `json:"id"`
	Name string `json:"name"`
}

type NodeCollection interface {
	Count() uint
	Nodes() []*Node
}

type Node struct {
	ID   uint   `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`

	Configuration NodeConfig `json:"config"`

	Inputs  Input    `json:"input"`
	Outputs []Output `json:"outputs"`
}

func (n *Node) Count() uint {
	return 1
}

func (n *Node) Nodes() []*Node {
	return []*Node{n}
}

func (n *Node) AddInput(i *Input) error {
	return errors.New("not implemented")
}

func (n *Node) AddOutput(o *Output) error {
	return errors.New("not implemented")
}

func (n *Node) ToGraphRef() (GraphRef, error) {
	return GraphRef(fmt.Sprintf("$ref:/node/%d", n.ID)), nil
}

func (n *Node) InputRefByName(name string) (GraphRef, error) {
	return "", nil
}

func (n *Node) OutputRefByName(name string) (GraphRef, error) {
	for _, o := range n.Outputs {
		if name == o.Name {
			return NewOutputGraphRef(n.ID, o.ID), nil
		}
	}
	return "", ErrorOutputNotFound
}

type Nodes []*Node

func (ns Nodes) Count() uint {
	return uint(len(ns))
}

func (ns Nodes) Nodes() []*Node {
	return ns
}
