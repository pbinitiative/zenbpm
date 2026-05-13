package rsync

import "sync"

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 | ~string
}

type ReadyTarget[T Ordered] struct {
	mu            sync.RWMutex
	currentTarget T
	subscribers   []*subscriber[T]
}

type subscriber[T Ordered] struct {
	target T
	ch     chan struct{}
}

func NewReadyTarget[T Ordered]() *ReadyTarget[T] {
	return &ReadyTarget[T]{
		subscribers: make([]*subscriber[T], 0),
	}
}

func (r *ReadyTarget[T]) Subscribe(target T) <-chan struct{} {
	ch := make(chan struct{})
	r.mu.Lock()
	defer r.mu.Unlock()
	if target <= r.currentTarget {
		close(ch)
	} else {
		r.subscribers = append(r.subscribers, &subscriber[T]{target: target, ch: ch})
	}
	return ch
}

func (r *ReadyTarget[T]) Unsubscribe(ch <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, s := range r.subscribers {
		if s.ch == ch {
			last := len(r.subscribers) - 1
			r.subscribers[i] = r.subscribers[last]
			r.subscribers[last] = nil
			r.subscribers = r.subscribers[:last]
			return
		}
	}
}

func (r *ReadyTarget[T]) Signal(index T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index <= r.currentTarget {
		return
	}
	r.currentTarget = index
	var remaining []*subscriber[T]
	for _, s := range r.subscribers {
		if index >= s.target {
			close(s.ch)
		} else {
			remaining = append(remaining, s)
		}
	}
	r.subscribers = remaining
}

func (r *ReadyTarget[T]) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	var zero T
	r.currentTarget = zero
	r.subscribers = make([]*subscriber[T], 0)
}

func (r *ReadyTarget[T]) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.subscribers)
}
