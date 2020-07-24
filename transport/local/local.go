package local

import (
	"context"
	"errors"
	//"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nirosys/gaufre/data"
	"github.com/nirosys/gaufre/transport"
)

// TODO: Limit size of queue, rather than letting it grow out of hand.
// TODO: Track stats about the queue, this will give us a much better view.
// TODO: Allow resize of queue? (ie. Free the underlying mem and reallocate a smaller one if not in  use?)

const (
	TransportTimeout time.Duration = 10 * time.Second
)

var ErrorTimeout = errors.New("timeout sending packet over local transport")

type LocalTransport struct {
	cond *sync.Cond

	errors    chan error
	packets   chan *data.InformationPacket
	reader_wg sync.WaitGroup
	queue     *WorkQueue
	closed    int32
}

func (lt *LocalTransport) Send(ctx context.Context, ip *data.InformationPacket) error {
	if lt.IsClosed() {
		return transport.ErrTransportClosed
	}
	err := lt.queue.Push(ip)
	if err != nil {
		return err
	} else {
		lt.cond.Signal()
		return nil
	}
}

func (lt *LocalTransport) reader() {
	defer lt.reader_wg.Done()
	for {
		lt.cond.L.Lock()
		for lt.queue.Len() == 0 && !lt.IsClosed() {
			lt.cond.Wait()
		}
		item, err := lt.queue.Pop()
		lt.cond.L.Unlock()

		if lt.IsClosed() {
			close(lt.packets)
			return
		} else if err != nil {
		}
		if item != nil {
			lt.packets <- item // Will block until read.
		}
	}
}

func (lt *LocalTransport) Receive(ctx context.Context) (*data.InformationPacket, error) {
	select {
	case item, ok := <-lt.packets:
		if !ok {
			return nil, transport.ErrTransportClosed
		}
		return item, nil
	case <-ctx.Done():
		return nil, ErrorTimeout
	}
}

func (lt *LocalTransport) IsClosed() bool {
	closed := atomic.LoadInt32(&lt.closed)
	return closed == 1
}

func (lt *LocalTransport) Errors() <-chan error {
	return lt.errors
}

func (lt *LocalTransport) Close() {
	atomic.StoreInt32(&lt.closed, 1)
	lt.cond.Signal()
}

func (lt *LocalTransport) Metrics() data.MetricCollection {
	return lt.queue.Metrics()
}

func NewLocalTransport() (*LocalTransport, error) {
	t := &LocalTransport{
		packets: make(chan *data.InformationPacket),
		cond:    sync.NewCond(&sync.Mutex{}),
		errors:  make(chan error),
		queue:   NewWorkQueue(),
	}
	t.reader_wg.Add(1)
	go t.reader()
	return t, nil
}
