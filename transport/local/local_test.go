package local

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nirosys/gaufre/data"
	"github.com/nirosys/gaufre/transport"
)

// This test ensures that we can receive what we send.. The most simple thing we can
// do.
func Test_Transport(t *testing.T) {
	transport, _ := NewLocalTransport()
	defer transport.Close()

	transport.Send(context.Background(), &data.InformationPacket{Type: data.PacketTypeData})

	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(TransportTimeout))
	item, err := transport.Receive(ctx)
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	}
	if item.Type != data.PacketTypeData {
		t.Errorf("packet received with altered type: %d != %d", item.Type, data.PacketTypeData)
	}
}

// This test tests that we will return after our deadline with an error indicating
// such. This is to verify that we won't have any forever blocking calls to Receive.
func Test_ErrorTimeout(t *testing.T) {
	transport, _ := NewLocalTransport()
	defer transport.Close()

	deadline := time.Now().Add(TransportTimeout)
	ctx, _ := context.WithDeadline(context.Background(), deadline)

	// No items in the queue, so our read should last TransportTimeout, and return an error.
	_, err := transport.Receive(ctx)
	end := time.Now()

	if !(end.Equal(deadline) || end.After(deadline)) { // We *should* be after, or equal to the deadline.
		t.Errorf("Returned before deadline: '%s' <= '%s'", end.String(), deadline.String())
	}

	if !errors.Is(err, ErrorTimeout) {
		t.Errorf("Expected Timeout Error, but got: %+v", err)
	}
}

// This tests ensures that our synchronization primitives aren't putting us
// into a deadlock when trying to access the queue with multiple go routines
// sending and receiving.
func Test_BlockingReceive(t *testing.T) {
	const SendDuration = 2 * time.Second

	transport, _ := NewLocalTransport()
	defer transport.Close()

	deadline := time.Now().Add(TransportTimeout)
	ctx, _ := context.WithDeadline(context.Background(), deadline)

	go func() {
		<-time.After(2 * time.Second)
		transport.Send(ctx, &data.InformationPacket{Type: data.PacketTypeData})
	}()

	// No items in the queue, so our read should last TransportTimeout, and return an error.
	item, err := transport.Receive(ctx)
	end := time.Now()
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}

	if !(end.Equal(deadline) || end.Before(deadline)) { // We *should* be before, or equal to the deadline.
		t.Errorf("Returned after deadline: '%s' <= '%s'", end.String(), deadline.String())
	}

	if item.Type != data.PacketTypeData {
		t.Errorf("Received packet has different type: %d != %d", item.Type, data.PacketTypeData)
	}

}

// This test is to reproduce an issue that came up while testing snitch..
//  The way the blocking receive was written, there was a chance that the
//  condition would signal, and by the time the channel communicated to the
//  primary go routine, the item would be gone.
func Test_MultiReader(t *testing.T) {
	trans, _ := NewLocalTransport()

	deadline := time.Now().Add(TransportTimeout)
	ctx, _ := context.WithDeadline(context.Background(), deadline)

	var wg sync.WaitGroup

	workerFunc := func() {
		defer wg.Done()
		for {
			item, err := trans.Receive(ctx)
			if errors.Is(err, transport.ErrTransportClosed) {
				return
			} else if err != nil {
				t.Errorf("unexpected error: %s", err.Error())
			} else if item == nil {
				t.Errorf("nil item returned")
			}
		}
	}

	for i := 0; i < 200; i++ { // More workers increases the chance of occurrence.
		go workerFunc()
		wg.Add(1)
	}

	for i := 0; i < 1000; i++ {
		trans.Send(ctx, &data.InformationPacket{Type: data.PacketTypeData})
	}
	trans.Close()

	t.Logf("waiting for workers..")
	wg.Wait()
}
