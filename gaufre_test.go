package gaufre_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nirosys/gaufre"
	"github.com/nirosys/gaufre/data"
	"github.com/nirosys/gaufre/graph"

	"github.com/rs/xid"
)

type flow struct {
	id     string
	ctx    context.Context
	closed bool
	mux    sync.RWMutex
}

func (f *flow) IsClosed() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()
	return f.closed
}

func (f *flow) Context() context.Context {
	return f.ctx
}

func (f *flow) ID() string {
	return f.id
}

func (f *flow) Error(err error) {
}

func (f *flow) Close() error {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.closed = true
	return nil
}

func newFlow(ctx context.Context, flowid string) data.Flow {
	return &flow{id: flowid, ctx: ctx}
}

type addNode struct{}

func (n *addNode) Configure(c *graph.NodeConfig) error {
	fmt.Printf("Configuring AddNode\n")
	return nil
}

func (n *addNode) HandleInfoPacket(rt *gaufre.Runtime, node *graph.Node, ip *data.InformationPacket) (*data.InformationPacket, error) {
	outputRef, err := node.OutputRefByName("output")
	if err != nil {
		return nil, err
	}

	// Handle flow errors
	if ip.Type&data.PacketTypeError != 0 {
		// Error packet, we need to forward iit..
		return &data.InformationPacket{
			Connection: graph.Connection{Start: outputRef},
			Type:       ip.Type,
		}, nil
	}

	var in_data int
	if ip.Data == nil {
		return nil, fmt.Errorf("nil data")
	} else if i, ok := ip.Data.(int); !ok {
		return nil, fmt.Errorf("Unexpected type from runtime")
	} else {
		in_data = i
	}

	out_ip := &data.InformationPacket{
		Connection: graph.Connection{Start: outputRef},
		Data:       in_data + 5,
		Type:       data.PacketTypeData | data.PacketTypeEndFlow,
	}
	return out_ip, nil
}

type errorNode struct{}

func (n *errorNode) Configure(c *graph.NodeConfig) error {
	fmt.Printf("Configuring ErrorNode\n")
	return nil
}

func (n *errorNode) HandleInfoPacket(rt *gaufre.Runtime, node *graph.Node, ip *data.InformationPacket) (*data.InformationPacket, error) {
	return nil, errors.New("test error")
}

func TestGraphExecution(t *testing.T) {
	myGraph := `
{"graph": {
  "name": "Compute 5 + 10 (Operations)",
  "nodes": [
    {
		 "id": 0,
		 "name": "Emit 5",
		 "type": "add",
		 "outputs": [
		    {"id": 0, "name": "output"}
		 ]
	 }
  ]
  }
}`
	cfg := *gaufre.DefaultConfig
	cfg.FlowFactory = newFlow
	rt, err := gaufre.NewRuntime(&cfg)
	if err != nil {
		t.Error(err)
	}
	rt.RegisterNodeType("add", &addNode{})

	defer rt.Shutdown()

	_, err = rt.Load(strings.NewReader(myGraph))
	if err != nil {
		t.Error(err)
	}

	log.Printf("Sending data.")
	rt.Send(context.Background(), 10) // Send 10, we expect 15 to come out the other end.

	log.Printf("Waiting on output")
	output := rt.Output()
	select {
	case o := <-output:
		if o == nil {
			t.Errorf("Nil data returned.")
		} else if i, ok := o.(int); !ok {
			t.Errorf("Unexpected type from runtime: %T", o)
		} else if i != 15 {
			t.Errorf("Unexpected value from runtime: %d != %d", i, 15)
		}
	case <-time.After(1 * time.Second):
		log.Printf("Did not receive value after 1 second")
		t.Errorf("Timed out waiting for result")
	}
	log.Printf("Done")
}

// Simple test to validate we can't accidentally add nodes with the same name.
func TestDuplicateNodeType(t *testing.T) {
	log.Printf("Testing duplicate node")

	cfg := *gaufre.DefaultConfig
	cfg.FlowFactory = newFlow
	rt, err := gaufre.NewRuntime(&cfg)
	if err != nil {
		t.Fatalf("Error creating runtime: %s", err)
	}
	defer rt.Shutdown()

	err = rt.RegisterNodeType("add", &addNode{})
	if err != nil {
		t.Errorf("Unexpected error adding node type: %s", err.Error())
	}
	err = rt.RegisterNodeType("add", &addNode{})
	if err == nil {
		t.Error("Unexpected success adding duplicate node type")
	} else if !errors.Is(err, gaufre.ErrorDuplicateNode) {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	log.Printf("duplicate done")
}

// This test simply tries to push as much data through a single node
// graph as possible.
func BenchmarkExecute(b *testing.B) {
	myGraph := `
{"graph": {
  "name": "Compute 5 + 10 (Operations)",
  "nodes": [
    {
		 "id": 0,
		 "name": "Emit 5",
		 "type": "add",
		 "outputs": [
		    {"id": 0, "name": "output"}
		 ]
	 }
  ]
  }
}`
	cfg := *gaufre.DefaultConfig
	cfg.FlowFactory = newFlow
	rt, err := gaufre.NewRuntime(&cfg)
	if err != nil {
		b.Error(err)
	}
	rt.RegisterNodeType("add", &addNode{})

	defer rt.Shutdown()

	_, err = rt.Load(strings.NewReader(myGraph))
	if err != nil {
		b.Error(err)
	}

	var wg sync.WaitGroup
	output := rt.Output()
	go func() {
		defer wg.Done()
		for {
			select {
			case _, ok := <-output:
				if !ok {
					return
				}
			case <-time.After(1 * time.Second):
				log.Printf("Did not receive value after 1 second")
			}
		}
	}()
	wg.Add(1)

	b.Run("Add 5", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rt.Send(context.Background(), 10)
		}
	})
}

func TestErrorPropagation(t *testing.T) {
	myGraph := `
{"graph": {
  "name": "Compute 5 + 10 (Operations)",
  "nodes": [
    {
		 "id": 0,
		 "name": "Error Node",
		 "type": "error",
		 "outputs": [
		    {"id": 0, "name": "output"}
		 ]
	 },
	 {
	    "id": 1,
		 "name": "Add Node",
		 "type": "add",
		 "outputs": [
		    {"id": 0, "name": "output"}
		 ]
	 }
  ],
  "connections": [
     {"start": "$ref:/node/0/output/0",
	   "end": "$ref:/node/1/input/0"}
  ]
  }
}`
	var ourFlow *flow
	cfg := *gaufre.DefaultConfig
	cfg.FlowFactory = func(ctx context.Context, flowid string) data.Flow {
		f := newFlow(ctx, flowid)
		ourFlow = f.(*flow)
		return f
	}
	rt, err := gaufre.NewRuntime(&cfg)
	if err != nil {
		t.Error(err)
	}
	rt.RegisterNodeType("add", &addNode{})
	rt.RegisterNodeType("error", &errorNode{})

	defer rt.Shutdown()

	_, err = rt.Load(strings.NewReader(myGraph))
	if err != nil {
		t.Error(err)
	}

	log.Printf("Sending data.")
	rt.Send(context.Background(), 10) // Send 10, we expect .. nothing..

	log.Printf("Waiting on output")
	output := rt.Output()
	select {
	case d, ok := <-output:
		if ok {
			t.Errorf("Received unexpected data: %+v", d)
		}
	case <-time.After(1 * time.Second):
	}
	<-time.After(500 * time.Millisecond)
	if !ourFlow.IsClosed() {
		t.Errorf("flow not closed")
	}
	log.Printf("Done")
}

// LEAF TESTS /////////////////////////////////////////////////////////////////

type leafFlow struct {
	id     string
	ctx    context.Context
	closed bool
	mux    sync.RWMutex
}

func (f *leafFlow) Context() context.Context {
	return f.ctx
}

func (f *leafFlow) ID() string {
	return f.id
}

func (f *leafFlow) Error(err error) {}

func (f *leafFlow) Close() error {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.closed = true
	return nil
}

func (f *leafFlow) IsClosed() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()
	return f.closed
}

var ourLeafFlow *leafFlow = nil

func newLeafFlow(ctx context.Context, flowid string) data.Flow {
	if ourLeafFlow == nil {
		ourLeafFlow = &leafFlow{id: flowid, ctx: ctx}
	}
	return ourLeafFlow
}

var leafSignalChan chan struct{} = make(chan struct{}, 1)

type leafNode struct{}

func (n *leafNode) Configure(c *graph.NodeConfig) error {
	fmt.Printf("Configuring leafNode\n")
	return nil
}

func (n *leafNode) HandleInfoPacket(rt *gaufre.Runtime, node *graph.Node, ip *data.InformationPacket) (*data.InformationPacket, error) {
	<-leafSignalChan

	outputRef, err := node.OutputRefByName("output")
	if err != nil {
		return nil, err
	}

	fmt.Printf("Leaf Node %d Done\n", node.ID)
	out_ip := &data.InformationPacket{
		Connection: graph.Connection{Start: outputRef},
		Type:       data.PacketTypeData | data.PacketTypeEndFlow,
	}

	return out_ip, nil
}

func TestEndFlows(t *testing.T) {
	myGraph := `
{"graph": {
  "name": "Nothing but leaves",
  "nodes": [
    {
		 "id": 0,
		 "name": "Emit 5",
		 "type": "add",
		 "outputs": [
		    {"id": 0, "name": "output"}
		 ]
	 },
    {
		 "id": 1,
		 "name": "Leaf Node 0",
		 "type": "leaf",
		 "outputs": [
		    {"id": 0, "name": "output"}
		 ]
	 },
	 {
	    "id": 2,
		 "name": "Leaf Node 1",
		 "type": "leaf",
		 "outputs": [
		    {"id": 0, "name": "output"}
		 ]
	 }
  ],
  "connections": [
     {"start": "$ref:/node/0/output/0",
	   "end": "$ref:/node/1/input/0"},
     {"start": "$ref:/node/0/output/0",
	   "end": "$ref:/node/2/input/0"}
  ]
}}`
	cfg := *gaufre.DefaultConfig
	cfg.FlowFactory = newLeafFlow
	rt, err := gaufre.NewRuntime(&cfg)
	if err != nil {
		t.Error(err)
	}
	rt.RegisterNodeType("add", &addNode{})
	rt.RegisterNodeType("leaf", &leafNode{})

	defer rt.Shutdown()

	_, err = rt.Load(strings.NewReader(myGraph))
	if err != nil {
		t.Error(err)
	}

	log.Printf("Sending Data")
	rt.Send(context.Background(), 10)

	// Signal first node to continue..
	leafSignalChan <- struct{}{}
	<-time.After(500 * time.Millisecond) // Wait some time for everything to resolve..
	if ourLeafFlow.IsClosed() {
		t.Errorf("Flow closed before second node finished")
	}

	leafSignalChan <- struct{}{}
	close(leafSignalChan)
	<-time.After(1500 * time.Millisecond)
	if !ourLeafFlow.IsClosed() {
		t.Errorf("Flow not closed after second node finished")
	}
}

// This test (once done) will help to verify that nothing strange happens with
// flows using NOP IPs.
/*
func TestEndFlowsWithNops(t *testing.T) {
	myGraph := `
{"graph": {
  "name": "Nothing but leaves",
  "nodes": [
    {
		 "id": 0,
		 "name": "Emit 5",
		 "type": "add",
		 "outputs": [
		    {"id": 0, "name": "output"}
		 ]
	 },
    {
		 "id": 1,
		 "name": "Leaf Node 0",
		 "type": "leaf",
		 "outputs": [
		    {"id": 0, "name": "output"}
		 ]
	 },
	 {
	    "id": 2,
		 "name": "Leaf Node 1",
		 "type": "leaf",
		 "outputs": [
		    {"id": 0, "name": "output"}
		 ]
	 }
  ],
  "connections": [
     {"start": "$ref:/node/0/output/0",
	   "end": "$ref:/node/1/input/0"},
     {"start": "$ref:/node/0/output/0",
	   "end": "$ref:/node/2/input/0"}
  ]
}}`
	cfg := *gaufre.DefaultConfig
	cfg.FlowFactory = newLeafFlow
	rt, err := gaufre.NewRuntime(&cfg)
	if err != nil {
		t.Error(err)
	}
	rt.RegisterNodeType("add", &addNode{})
	rt.RegisterNodeType("leaf", &leafNode{})

	defer rt.Shutdown()

	_, err = rt.Load(strings.NewReader(myGraph))
	if err != nil {
		t.Error(err)
	}

	log.Printf("Sending Data")

	// TODO: instead of using Send we need to somehow deliver an IP to the root
	//       node with a pre-defined flowid.. so we can have the node issue a nop,
	//       and then emit data on a particular value.
	rt.Send(context.Background(), 10)

	// Signal first node to continue..
	leafSignalChan <- struct{}{}
	<-time.After(500 * time.Millisecond) // Wait some time for everything to resolve..
	if ourLeafFlow.IsClosed() {
		t.Errorf("Flow closed before second node finished")
	}

	leafSignalChan <- struct{}{}
	close(leafSignalChan)
	<-time.After(1500 * time.Millisecond)
	if !ourLeafFlow.IsClosed() {
		t.Errorf("Flow not closed after second node finished")
	}
}
*/

// Simple test to make sure we don't get duplicate IDs.
func TestFlowID(t *testing.T) {
	const MAX_ITER = 2000
	ids := map[string]struct{}{}
	for i := 0; i < MAX_ITER; i++ {
		id := xid.New().String()
		if _, have := ids[id]; have {
			t.Errorf("Have ID already: %s", id)
		} else {
			ids[id] = struct{}{}
		}
	}
	if len(ids) != MAX_ITER {
		t.Errorf("Did not create enough IDs: %d != %d", len(ids), MAX_ITER)
	}
}
