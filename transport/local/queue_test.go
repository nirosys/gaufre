package local

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nirosys/gaufre/data"
)

type flow struct {
	ctx context.Context
	id  string
}

func (f *flow) Context() context.Context {
	return f.ctx
}

func (f *flow) ID() string {
	return f.id
}

func (f *flow) Error(error) {}

func (f *flow) Close() error {
	return nil
}

func Test_QueuePush(t *testing.T) {
	wq := NewWorkQueue()
	err := wq.Push(&data.InformationPacket{})
	if err != nil {
		t.Errorf("Error pushing data: %s\n", err.Error())
	}
	if wq.Len() != 1 {
		t.Errorf("Unexpected length: %d != 1", wq.Len())
	}

	metrics := wq.Metrics()
	if v, ok := metrics["num_pushes"]; ok && v != 1 {
		t.Errorf("Unexpected push count: %d != 1", v)
	}
}

func Test_QueuePopEmpty(t *testing.T) {
	wq := NewWorkQueue()
	item, err := wq.Pop()
	if err != nil {
		t.Errorf("Unexpected non-nil error.")
	}
	if item != nil {
		t.Errorf("Unexpected non-nil item.")
	}

	metrics := wq.Metrics()
	if v, ok := metrics["num_pops"]; ok && v != 0 {
		t.Errorf("Unexpected pop count: %d != 0", v)
	}
}

func Test_QueuePop(t *testing.T) {
	wq := NewWorkQueue()
	//ctx := context.WithValue(context.Background(), "value", "foo")

	f := &flow{ctx: context.Background(), id: "foo"}

	err := wq.Push(&data.InformationPacket{Flow: f})
	if err != nil {
		t.Errorf("Error pushing data: %s", err.Error())
	}
	if wq.Len() != 1 {
		t.Errorf("Unexpected length: %d != 1", wq.Len())
	}

	item, err := wq.Pop()
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if item == nil {
		t.Errorf("Unexpected nil item")
	}

	id := item.Flow.ID()
	if id != "foo" {
		t.Errorf("Mismatched ID '%s' != 'foo'", id)
	}
	//v, ok := item.Context.Value("value").(string)
	//if !ok {
	//	t.Errorf("Missing string value in context")
	//} else if v != "foo" {
	//	t.Errorf("Unexpected value: '%s' != 'foo'", v)
	//}

	metrics := wq.Metrics()

	if v, ok := metrics["num_pushes"]; ok && v != 1 {
		t.Errorf("Unexpected push count: %d != 1", v)
	}

	if v, ok := metrics["num_pops"]; ok && v != 1 {
		t.Errorf("Unexpected pop count: %d != 1", v)
	}
}

func Test_QueuePriority(t *testing.T) {
	wq := NewWorkQueue()
	priorities := []int{0, 1, -1, 1}

	for _, p := range priorities {
		f := &flow{ctx: context.Background(), id: "foo"}

		err := wq.Push(&data.InformationPacket{Flow: f, Priority: p})
		if err != nil {
			t.Errorf("unexpected error pushing packet: %s", err.Error())
		}
	}
	expected := []struct {
		Priority int
		Index    int
	}{
		{Priority: 1},
		{Priority: 1},
		{Priority: 0},
		{Priority: -1},
	}

	for _, e := range expected {
		item, err := wq.Pop()
		if err != nil {
			t.Errorf("Unexpected error: %s", err.Error())
		}
		fmt.Printf("%d == %d\n", item.Priority, e.Priority)
		if item.Priority != e.Priority {
			t.Errorf("Unexpected item Priority: %d != %d", item.Priority, e.Priority)
		}
	}

	metrics := wq.Metrics()

	if v, ok := metrics["num_pushes"]; !ok || v != len(priorities) {
		t.Errorf("Unexpected push count: %d != %d", v, len(priorities))
	}

	if v, ok := metrics["num_pops"]; !ok || v != len(priorities) {
		t.Errorf("Unexpected pop count: %d != %d", v, len(priorities))
	}
}

// Basic benchmark to test throughput of the queue with 1 reader and 1 writer.
func Benchmark_Throughput(b *testing.B) {
	wq := NewWorkQueue()
	ip := data.InformationPacket{Type: data.PacketTypeData, Priority: 1}
	var wg sync.WaitGroup

	var count = 0

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-time.After(200 * time.Millisecond)
	LOOP:
		for {
			if ip, _ := wq.Pop(); ip != nil {
				if ip.Type == data.PacketTypeShutdown {
					break LOOP
				} else {
					count = count + 1
				}
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wq.Push(&ip)
	}
	wq.Push(&data.InformationPacket{Type: data.PacketTypeShutdown, Priority: 0})
	wg.Wait()
	if count != b.N {
		b.Errorf("Popped count does not equal sent count: %d != %d", count, b.N)
	}
}
