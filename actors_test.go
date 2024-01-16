package main

import (
	"fmt"
	"hash"
	"hash/crc32"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anthdm/hollywood/actor"
	"github.com/google/uuid"
)

const (
	NUMBER_OF_ITEMS  = 16384
	NUMBER_OF_ACTORS = 8
	QUEUE_SIZE       = 128
	MSG_WITH_WAIT    = 0
	MSG_WITHOUT_WAIT = 1
	MSG_CLOSE        = 2
	WAIT_DURATION    = 5 * time.Microsecond
)

var counter int64

type Message struct {
	uuid string
	kind int8
	data int64
}

type HollywoodActor struct {
	processedMsgs int
}

func (h *HollywoodActor) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case *Message:
		if msg.kind == MSG_WITH_WAIT {
			time.Sleep(WAIT_DURATION)
		}
		atomic.AddInt64(&counter, msg.data)
		h.processedMsgs++
	case actor.Stopped:
		// fmt.Printf("actor %s stopped, processed: %d \n", ctx.PID().ID, h.processedMsgs)
	}
}

func channelActorFunc(ch <-chan interface{}, pid int, wg *sync.WaitGroup) {
	defer wg.Done()
	var msgProcessed int
	for v := range ch {
		msg, ok := v.(*Message)
		if ok {
			func() {
				defer func() { // this is here for higher fidelity, I don't think it makes any noticeable difference
					if r := recover(); r != nil {
						fmt.Println("recovered: ", r)
					}
				}()
				if msg.kind == MSG_WITH_WAIT {
					time.Sleep(WAIT_DURATION)
				}
				atomic.AddInt64(&counter, msg.data)
				msgProcessed++
			}()
		}
	}
	// fmt.Printf("actor %d stopped, processed: %d\n", pid, msgProcessed)
}

func mpscActorFunc(q *Queue, pid int, wg *sync.WaitGroup) {
	defer wg.Done()
	var msgProcessed int
	for {
		m := q.Pop()
		if m == nil {
			runtime.Gosched()
			continue
		} else {
			msg, ok := m.(*Message)
			if ok {
				shouldContinue := func() bool {
					defer func() { // this is here for higher fidelity, I don't think it makes any noticeable difference
						if r := recover(); r != nil {
							fmt.Println("recovered: ", r)
						}
					}()
					if msg.kind == MSG_CLOSE {
						return false
					}
					if msg.kind == MSG_WITH_WAIT {
						time.Sleep(WAIT_DURATION)
					}
					atomic.AddInt64(&counter, msg.data)
					msgProcessed++
					return true
				}()
				if !shouldContinue {
					return
				}
			} else {
				fmt.Println(reflect.TypeOf(m))
			}
		}
	}
	// fmt.Printf("actor %d stopped, processed: %d\n", pid, msgProcessed)
}

func newHollywoodActor() actor.Receiver {
	return &HollywoodActor{}
}

func benchmarkHollywood(b *testing.B, withWait bool) {
	engine, err := actor.NewEngine(actor.NewEngineConfig())
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		counter = 0
		actors := make([]*actor.PID, NUMBER_OF_ACTORS)
		for i := 0; i < NUMBER_OF_ACTORS; i++ {
			actors[i] = engine.Spawn(newHollywoodActor, "test_actor", actor.WithInboxSize(QUEUE_SIZE), actor.WithID(strconv.Itoa(i)))
		}
		for i := 0; i < NUMBER_OF_ITEMS; i++ {
			uuid := uuid.New().String()
			actorIndex := consistentHashCRC32(uuid, NUMBER_OF_ACTORS)
			if withWait {
				engine.SendLocal(actors[actorIndex], &Message{uuid: uuid, kind: MSG_WITH_WAIT, data: int64(1)}, nil)
			} else {
				engine.SendLocal(actors[actorIndex], &Message{uuid: uuid, kind: MSG_WITHOUT_WAIT, data: int64(1)}, nil)
			}
		}

		var actorWg sync.WaitGroup
		for i := 0; i < NUMBER_OF_ACTORS; i++ {
			engine.Poison(actors[i], &actorWg)
		}
		actorWg.Wait()
		// b.Log("counter_hollywood: ", counter)
	}
}

func BenchmarkHollywoodWithWait(b *testing.B) {
	benchmarkHollywood(b, true)
}

func BenchmarkHollywoodWithoutWait(b *testing.B) {
	benchmarkHollywood(b, false)
}

func benchmarkChannels(b *testing.B, withWait bool) {
	for n := 0; n < b.N; n++ {
		counter = 0
		var wg sync.WaitGroup
		actors := make([]chan<- interface{}, NUMBER_OF_ACTORS)
		for i := 0; i < NUMBER_OF_ACTORS; i++ {
			actors[i] = newChannelActor(channelActorFunc, i, QUEUE_SIZE, &wg)
		}
		for i := 0; i < NUMBER_OF_ITEMS; i++ {
			uuid := uuid.New().String()
			actorIndex := consistentHashCRC32(uuid, NUMBER_OF_ACTORS)
			if withWait {
				actors[actorIndex] <- &Message{uuid: uuid, kind: MSG_WITH_WAIT, data: int64(1)}
			} else {
				actors[actorIndex] <- &Message{uuid: uuid, kind: MSG_WITHOUT_WAIT, data: int64(1)}
			}
		}
		for i := 0; i < NUMBER_OF_ACTORS; i++ {
			close(actors[i])
		}
		wg.Wait()
		// b.Log("counter_channels: ", counter)
	}
}

func benchmarkMpscChannels(b *testing.B, withWait bool) {
	for n := 0; n < b.N; n++ {
		counter = 0
		var wg sync.WaitGroup
		actors := make([]*Queue, NUMBER_OF_ACTORS)
		for i := 0; i < NUMBER_OF_ACTORS; i++ {
			actors[i] = newMpscActor(mpscActorFunc, i, QUEUE_SIZE, &wg)
		}
		for i := 0; i < NUMBER_OF_ITEMS; i++ {
			uuid := uuid.New().String()
			actorIndex := consistentHashCRC32(uuid, NUMBER_OF_ACTORS)
			if withWait {
				actors[actorIndex].Push(&Message{uuid: uuid, kind: MSG_WITH_WAIT, data: int64(1)})
			} else {
				actors[actorIndex].Push(&Message{uuid: uuid, kind: MSG_WITHOUT_WAIT, data: int64(1)})
			}
		}
		for i := 0; i < NUMBER_OF_ACTORS; i++ {
			actors[i].Push(&Message{uuid: "0", kind: MSG_CLOSE, data: int64(0)})
		}
		wg.Wait()
		// b.Log("counter_channels: ", counter)
	}
}

func BenchmarkChannelsWithWait(b *testing.B) {
	benchmarkChannels(b, true)
}

func BenchmarkChannelsWithoutWait(b *testing.B) {
	benchmarkChannels(b, false)
}

func BenchmarkMpscChannelsWithWait(b *testing.B) {
	benchmarkMpscChannels(b, true)
}

func BenchmarkMpscChannelsWithoutWait(b *testing.B) {
	benchmarkMpscChannels(b, false)
}

func newChannelActor(f func(ch <-chan interface{}, pid int, wg *sync.WaitGroup), pid int, queueSize int, wg *sync.WaitGroup) chan<- interface{} {
	ch := make(chan interface{}, queueSize)
	wg.Add(1)
	go f(ch, pid, wg)
	return ch
}

func newMpscActor(f func(q *Queue, pid int, wg *sync.WaitGroup), pid int, queueSize int, wg *sync.WaitGroup) *Queue {
	q := NewQueue()
	wg.Add(1)
	go f(q, pid, wg)
	return q
}

var hasherPool = &sync.Pool{
	New: func() interface{} {
		return crc32.NewIEEE()
	},
}

func consistentHashCRC32(key string, bucketCount int) int {
	hasher := hasherPool.Get().(hash.Hash32)
	defer hasherPool.Put(hasher)
	hasher.Reset()
	hasher.Write([]byte(key))
	hash := hasher.Sum32()
	return int(hash) % bucketCount
}
