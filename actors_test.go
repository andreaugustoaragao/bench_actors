package agent

import (
	"fmt"
	"hash/fnv"
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
	WAIT_DURATION    = 5 * time.Millisecond
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
		// fmt.Println("actor stopped, processed: ", h.processedMsgs)
	}
}

func channelActorFunc(ch <-chan interface{}, pid int, wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() { // this is here for higher fidelity, I don't think it makes any noticeable difference
		if r := recover(); r != nil {
			fmt.Println("recovered: ", r)
		}
	}()
	var msgProcessed int
	for v := range ch {
		msg, ok := v.(*Message)
		if ok {
			if msg.kind == MSG_WITH_WAIT {
				time.Sleep(WAIT_DURATION)
			}
			atomic.AddInt64(&counter, msg.data)
			msgProcessed++
		}
	}
	// fmt.Println("actor stopped, processed: ", msgProcessed)
}

func newHollywoodActor() actor.Receiver {
	return &HollywoodActor{}
}

func benchmarkHollywood(b *testing.B, withWait bool) {
	engine, err := actor.NewEngine(actor.NewEngineConfig())
	if err != nil {
		b.Fatal(err)
	}
	actors := make([]*actor.PID, NUMBER_OF_ACTORS)
	for i := 0; i < NUMBER_OF_ACTORS; i++ {
		actors[i] = engine.Spawn(newHollywoodActor, strconv.Itoa(i), actor.WithInboxSize(QUEUE_SIZE))
	}
	counter = 0
	for n := 0; n < b.N; n++ {
		var sendSync sync.WaitGroup
		var i int64
		counter = 0
		for i = 0; i < NUMBER_OF_ITEMS; i++ {
			sendSync.Add(1)
			go func() {
				defer sendSync.Done()
				uuid := uuid.New().String()
				hash := fnv.New32a()
				hash.Write([]byte(uuid))
				actorIndex := int(hash.Sum32()) % NUMBER_OF_ACTORS
				if withWait {
					engine.Send(actors[actorIndex], &Message{uuid: uuid, kind: MSG_WITH_WAIT, data: int64(1)})
				} else {
					engine.Send(actors[actorIndex], &Message{uuid: uuid, kind: MSG_WITHOUT_WAIT, data: int64(1)})
				}
			}()
		}
		sendSync.Wait()
	}
	var actorWg sync.WaitGroup
	for i := 0; i < NUMBER_OF_ACTORS; i++ {
		engine.Poison(actors[i], &actorWg)
	}
	actorWg.Wait()
	// b.Log("counter_hollywood: ", counter)
}

func BenchmarkHollywoodWithWait(b *testing.B) {
	benchmarkHollywood(b, true)
}

func BenchmarkHollywoodWithoutWait(b *testing.B) {
	benchmarkHollywood(b, false)
}

func benchmarkChannels(b *testing.B, withWait bool) {
	counter = 0
	var wg sync.WaitGroup
	actors := make([]chan<- interface{}, NUMBER_OF_ACTORS)
	for i := 0; i < NUMBER_OF_ACTORS; i++ {
		actors[i] = newChannelActor(channelActorFunc, i, QUEUE_SIZE, &wg)
	}
	counter = 0
	for n := 0; n < b.N; n++ {
		var sendSync sync.WaitGroup
		var i int64
		for i = 0; i < NUMBER_OF_ITEMS; i++ {
			sendSync.Add(1)
			go func() {
				defer sendSync.Done()
				uuid := uuid.New().String()
				hash := fnv.New32a()
				hash.Write([]byte(uuid))
				actorIndex := int(hash.Sum32()) % NUMBER_OF_ACTORS
				if withWait {
					actors[actorIndex] <- &Message{uuid: uuid, kind: MSG_WITH_WAIT, data: int64(1)}
				} else {
					actors[actorIndex] <- &Message{uuid: uuid, kind: MSG_WITHOUT_WAIT, data: int64(1)}
				}
			}()
		}
		sendSync.Wait()
	}
	for i := 0; i < NUMBER_OF_ACTORS; i++ {
		close(actors[i])
	}
	wg.Wait()
	// b.Log("counter_channels: ", counter)
}

func BenchmarkChannelsWithWait(b *testing.B) {
	benchmarkChannels(b, true)
}

func BenchmarkChannelsWithoutWait(b *testing.B) {
	benchmarkChannels(b, false)
}

func newChannelActor(f func(ch <-chan interface{}, pid int, wg *sync.WaitGroup), pid int, queueSize int, wg *sync.WaitGroup) chan<- interface{} {
	ch := make(chan interface{}, queueSize)
	wg.Add(1)
	go f(ch, pid, wg)
	return ch
}
