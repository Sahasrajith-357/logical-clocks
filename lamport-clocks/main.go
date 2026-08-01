package main

import (
	"fmt"
	"sort"
	"sync"
)

type EventType int

const (
	Internal EventType = iota
	Send
	Receive
)

type EventMap map[EventType]string

var eventTypeNames = EventMap{
	Internal: "internal",
	Send:     "send",
	Receive:  "receive",
}

// struct that represents a message in the Lamport clock system.
type Message struct {
	from      int
	timestamp int
	payload   string
}

// struct that represents an event in the Lamport clock system, logged for the final ordering of events.
type Event struct {
	processId int
	clock     int
	kind      EventType
	detail    string
}

// a shared sink to which every process appends its events.
// the append order is the real ordering of events.
// protected by a mutex since multiple processes can append to it concurrently.
type Logger struct {
	mu     sync.Mutex
	events []Event
}

func (l *Logger) record(e Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

type Process struct {
	id    int
	clock int
	mu    sync.Mutex
	inbox chan Message
	log   *Logger
	peers map[int]chan Message // for tracking the inboxes the process sends messages to
}

// internal event: just increments the clock and logs the event.
func (p *Process) internal(detail string) {
	p.mu.Lock()
	p.clock++
	p.mu.Unlock()
	p.log.record(Event{
		processId: p.id,
		clock:     p.clock,
		kind:      Internal,
		detail:    detail,
	})
}

// send event: increments the clock, sends a message with timestamp to another process, and logs the event.
func (p *Process) send(to int, payload string) {
	p.mu.Lock()
	p.clock++
	timestamp := p.clock
	p.mu.Unlock()

	msg := Message{
		from:      p.id,
		timestamp: timestamp,
		payload:   payload,
	}
	p.peers[to] <- msg

	p.log.record(Event{
		processId: p.id,
		clock:     p.clock,
		kind:      Send,
		detail:    fmt.Sprintf("%q -> P%d", payload, to),
	})
}

// receive event: blocks until a message is received, updates the clock based on the received timestamp, and logs the event.
func (p *Process) receive() {
	msg := <-p.inbox // blocks until a message is received
	p.mu.Lock()
	if msg.timestamp > p.clock {
		p.clock = msg.timestamp
	}
	p.clock++
	p.mu.Unlock()

	p.log.record(Event{
		processId: p.id,
		clock:     p.clock,
		kind:      Receive,
		detail:    fmt.Sprintf("%q from P%d", msg.payload, msg.from),
	})
}

func main() {
	inbox1 := make(chan Message, 10)
	inbox2 := make(chan Message, 10)
	inbox3 := make(chan Message, 10)

	logger := &Logger{}
	peers := map[int]chan Message{
		1: inbox1,
		2: inbox2,
		3: inbox3,
	}

	var wg sync.WaitGroup
	wg.Add(3)

	// create three processes with their respective inboxes and the shared logger
	p1 := &Process{
		id:    1,
		inbox: inbox1,
		log:   logger,
		peers: peers,
	}

	p2 := &Process{
		id:    2,
		inbox: inbox2,
		log:   logger,
		peers: peers,
	}

	p3 := &Process{
		id:    3,
		inbox: inbox3,
		log:   logger,
		peers: peers,
	}

	go func() {
		defer wg.Done()
		p1.internal("boot")
		p1.send(2, "hello P2, I am P1")
		p1.internal("processing")
		p1.send(3, "hello P3, I am P1")
		p1.internal("shutting down")
	}()

	go func() {
		defer wg.Done()
		p2.internal("boot")
		p2.receive()
		p2.internal("processing")
		p2.send(3, "hello P3, I am P2")
		p2.internal("shutting down")
	}()

	go func() {
		defer wg.Done()
		p3.internal("boot")
		p3.receive()
		p3.receive()
		p3.internal("processing")
		p3.send(2, "hello P2, I am P3")
		p3.internal("shutting down")
	}()

	wg.Wait()

	// printing the final event log in the order they were recorded
	fmt.Println("Final event log:")
	for _, event := range logger.events {
		fmt.Printf("P%d  clock=%d  %s 	 %s\n", event.processId, event.clock, eventTypeNames[event.kind], event.detail)
	}

	sort.Slice(logger.events, func(i, j int) bool {
		if logger.events[i].clock == logger.events[j].clock {
			return logger.events[i].processId < logger.events[j].processId
		}
		return logger.events[i].clock < logger.events[j].clock
	})

	// printing the final event log in the order of Lamport clocks
	fmt.Println("\nFinal event log (sorted by Lamport clocks):")
	for _, event := range logger.events {
		fmt.Printf("P%d  clock=%d  %s 	 %s\n", event.processId, event.clock, eventTypeNames[event.kind], event.detail)
	}
}
