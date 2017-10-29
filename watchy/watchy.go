package watchy

import (
	"sync"
)

type EventEmitter struct {
	listeners map[string][]Listener
	mu        sync.RWMutex
}

type Listener struct {
	ch   chan interface{}
	once bool
}

func New() *EventEmitter {
	return &EventEmitter{
		listeners: map[string][]Listener{},
	}
}

func (r *EventEmitter) Emit(event string, payload interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < len(r.listeners[event]); i++ {
		l := r.listeners[event][i]
		select {
		case l.ch <- payload:
		default:
		}
		if l.once { // remove
			r.listeners[event] = append(r.listeners[event][:i], r.listeners[event][i+1:]...)
			i--
		}
	}
}

func (r *EventEmitter) Once(event string) chan interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan interface{}, 1)

	r.listeners[event] = append(r.listeners[event], Listener{
		ch:   ch,
		once: true,
	})
	return ch
}

func (r *EventEmitter) On(event string) chan interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan interface{}, 1)

	r.listeners[event] = append(r.listeners[event], Listener{
		ch:   ch,
		once: false,
	})
	return ch
}
