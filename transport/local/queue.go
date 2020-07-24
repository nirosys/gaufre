package local

// This file contains an implementation of a simple priority queue based on
// Go's heap container.
// All access should be through a WorkQueue.

import (
	"container/heap"
	"sync"

	"github.com/nirosys/gaufre/data"
)

type sortedQueue []*data.InformationPacket

func (sq sortedQueue) Len() int { return len(sq) }

func (sq sortedQueue) Less(i, j int) bool {
	return sq[i].Priority > sq[j].Priority
}

func (sq sortedQueue) Swap(i, j int) {
	sq[i], sq[j] = sq[j], sq[i]
}

func (sq *sortedQueue) Push(x interface{}) {
	*sq = append(*sq, x.(*data.InformationPacket))
}

func (sq *sortedQueue) Pop() interface{} {
	old := *sq
	n := len(old)
	if n > 0 {
		item := old[n-1]
		*sq = old[0 : n-1]
		return item
	} else {
		return nil
	}
}

type WorkQueueMetrics struct {
	NumPushes int
	NumPops   int
}

type WorkQueue struct {
	mutex sync.Mutex

	data    sortedQueue
	metrics *WorkQueueMetrics
}

func (wq *WorkQueue) Metrics() data.MetricCollection {
	wq.mutex.Lock()
	defer wq.mutex.Unlock()

	metrics := map[string]interface{}{
		"num_pushes": wq.metrics.NumPushes,
		"num_pops":   wq.metrics.NumPops,
		"capacity":   cap(wq.data),
	}
	wq.metrics = &WorkQueueMetrics{}
	return metrics
}

func (wq *WorkQueue) Push(ip *data.InformationPacket) error {
	wq.mutex.Lock()
	defer wq.mutex.Unlock()

	heap.Push(&wq.data, ip)
	wq.metrics.NumPushes += 1
	return nil
}

func (wq *WorkQueue) Pop() (*data.InformationPacket, error) {
	wq.mutex.Lock()
	defer wq.mutex.Unlock()

	if len(wq.data) > 0 {
		ip := heap.Pop(&wq.data).(*data.InformationPacket)
		wq.metrics.NumPops += 1
		return ip, nil
	} else {
		return nil, nil
	}
}

func (wq *WorkQueue) Len() int {
	wq.mutex.Lock()
	defer wq.mutex.Unlock()

	return len(wq.data)
}

func NewWorkQueue() *WorkQueue {
	wq := WorkQueue{
		data:    make(sortedQueue, 0, 1000),
		metrics: &WorkQueueMetrics{},
	}
	heap.Init(&wq.data)
	return &wq
}
