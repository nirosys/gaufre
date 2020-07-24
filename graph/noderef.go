package graph

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrorInvalidGraphReference = errors.New("Invalid graph reference")
var ErrorInvalidGraphRefType = errors.New("Invalid reference type")

type GraphReferable interface {
	ToGraphRef() (GraphRef, error)
}

type GraphRefType uint

const (
	GraphRefTypeUnknown    GraphRefType = 0
	GraphRefTypeNode       GraphRefType = 1
	GraphRefTypeConnection GraphRefType = 2
	GraphRefTypeInput      GraphRefType = 3
	GraphRefTypeOutput     GraphRefType = 4
)

type GraphRef string

func NewNodeGraphRef(node uint) GraphRef {
	return GraphRef(fmt.Sprintf("$ref:/node/%d", node))
}

func NewInputGraphRef(node, socket uint) GraphRef {
	return GraphRef(fmt.Sprintf("$ref:/node/%d/input/%d", node, socket))
}

func NewOutputGraphRef(node, socket uint) GraphRef {
	return GraphRef(fmt.Sprintf("$ref:/node/%d/output/%d", node, socket))
}

func (r GraphRef) Type() (GraphRefType, error) {
	toks := strings.Split(strings.ToLower(string(r)), "/")
	return ref_type(toks)
}

func ref_type(toks []string) (GraphRefType, error) {
	if len(toks) < 2 || toks[0] != "$ref:" {
		return GraphRefTypeUnknown, errors.New("Invalid reference")
	}

	if ntok := len(toks); ntok == 3 && toks[1] == "node" {
		return GraphRefTypeNode, nil
	} else if ntok == 5 && toks[3] == "input" {
		return GraphRefTypeInput, nil
	} else if ntok == 5 && toks[3] == "output" {
		return GraphRefTypeOutput, nil
	} else {
		fmt.Printf("toks: %+v\n", toks)
		return GraphRefTypeUnknown, ErrorInvalidGraphReference
	}
}

func (r GraphRef) NodeRef() (GraphRef, error) {
	if id, err := r.NodeId(); err != nil {
		return "", err
	} else {
		return NewNodeGraphRef(id), nil
	}
}

func (r GraphRef) NodeId() (uint, error) {
	toks := strings.Split(strings.ToLower(string(r)), "/")
	return ref_nodeId(toks)
}

func ref_nodeId(toks []string) (uint, error) {
	if len(toks) < 3 || toks[0] != "$ref:" {
		return 0, ErrorInvalidGraphReference
	}

	if _, err := ref_type(toks); err != nil {
		return 0, err
	} else if toks[1] != "node" {
		return 0, ErrorInvalidGraphRefType
	}

	id, err := strconv.ParseUint(toks[2], 10, 32)
	if err != nil {
		return 0, err
	}

	return uint(id), nil
}

func (r GraphRef) SocketId() (uint, error) {
	toks := strings.Split(strings.ToLower(string(r)), "/")
	if tpe, err := ref_type(toks); err != nil {
		return 0, err
	} else if tpe != GraphRefTypeOutput && tpe != GraphRefTypeInput {
		return 0, ErrorInvalidGraphRefType
	}

	id, err := strconv.ParseUint(toks[4], 10, 32)
	if err != nil {
		return 0, err
	}
	return uint(id), nil
}
