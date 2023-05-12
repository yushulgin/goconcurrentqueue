package goconcurrentqueue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// FIFO (First In First Out) concurrent queue
type TFIFO[T any] struct {
	slice       []T
	rwmutex     sync.RWMutex
	lockRWmutex sync.RWMutex
	isLocked    bool
	// queue for watchers that will wait for next elements (if queue is empty at DequeueOrWaitForNextElement execution )
	waitForNextElementChan chan chan T
	// queue to unlock consumers that were locked when queue was empty (during DequeueOrWaitForNextElement execution)
	unlockDequeueOrWaitForNextElementChan chan struct{}
}

// NewFIFO returns a new FIFO concurrent queue
func NewTFIFO[T any]() *TFIFO[T] {
	ret := &TFIFO[T]{}
	ret.initialize()

	return ret
}

func (st *TFIFO[T]) initialize() {
	st.slice = make([]T, 0)
	st.waitForNextElementChan = make(chan chan T, WaitForNextElementChanCapacity)
	st.unlockDequeueOrWaitForNextElementChan = make(chan struct{}, WaitForNextElementChanCapacity)
}

// Enqueue enqueues an element. Returns error if queue is locked.
func (st *TFIFO[T]) Enqueue(value T) error {
	if st.isLocked {
		return NewQueueError(QueueErrorCodeLockedQueue, "The queue is locked")
	}

	// let consumers (DequeueOrWaitForNextElement) know there is a new element
	select {
	case st.unlockDequeueOrWaitForNextElementChan <- struct{}{}:
	default:
		// message could not be sent
	}

	// check if there is a listener waiting for the next element (this element)
	select {
	case listener := <-st.waitForNextElementChan:
		// send the element through the listener's channel instead of enqueue it
		select {
		case listener <- value:
		default:
			// enqueue if listener is not ready

			// lock the object to enqueue the element into the slice
			st.rwmutex.Lock()
			// enqueue the element
			st.slice = append(st.slice, value)
			defer st.rwmutex.Unlock()
		}

	default:
		// lock the object to enqueue the element into the slice
		st.rwmutex.Lock()
		// enqueue the element
		st.slice = append(st.slice, value)
		defer st.rwmutex.Unlock()
	}

	return nil
}

// Dequeue dequeues an element. Returns error if queue is locked or empty.
func (st *TFIFO[T]) Dequeue() (T, error) {
	var emptyRes T
	if st.isLocked {
		return emptyRes, NewQueueError(QueueErrorCodeLockedQueue, "The queue is locked")
	}

	st.rwmutex.Lock()
	defer st.rwmutex.Unlock()

	length := len(st.slice)
	if length == 0 {
		return emptyRes, NewQueueError(QueueErrorCodeEmptyQueue, "empty queue")
	}

	elementToReturn := st.slice[0]
	st.slice = st.slice[1:]

	return elementToReturn, nil
}

// Dequeue dequeues an N count element. Returns error if queue is locked or empty.
func (st *TFIFO[T]) DequeueN(count uint) ([]T, error) {
	var emptyRes []T
	if st.isLocked {
		return emptyRes, NewQueueError(QueueErrorCodeLockedQueue, "The queue is locked")
	}

	st.rwmutex.Lock()
	defer st.rwmutex.Unlock()

	lenSlice := len(st.slice)
	if lenSlice == 0 {
		return emptyRes, NewQueueError(QueueErrorCodeEmptyQueue, "empty queue")
	}

	if uint(lenSlice) < count {
		count = uint(lenSlice)
	}
	elementsToReturn := st.slice[:count]
	st.slice = st.slice[len(elementsToReturn):]

	return elementsToReturn, nil
}

// DequeueOrWaitForNextElement dequeues an element (if exist) or waits until the next element gets enqueued and returns it.
// Multiple calls to DequeueOrWaitForNextElement() would enqueue multiple "listeners" for future enqueued elements.
func (st *TFIFO[T]) DequeueOrWaitForNextElement() (T, error) {
	return st.DequeueOrWaitForNextElementContext(context.Background())
}

// DequeueOrWaitForNextElementContext dequeues an element (if exist) or waits until the next element gets enqueued and returns it.
// Multiple calls to DequeueOrWaitForNextElementContext() would enqueue multiple "listeners" for future enqueued elements.
// When the passed context expires this function exits and returns the context' error
func (st *TFIFO[T]) DequeueOrWaitForNextElementContext(ctx context.Context) (T, error) {
	var emptyRes T
	for {
		if st.isLocked {
			return emptyRes, NewQueueError(QueueErrorCodeLockedQueue, "The queue is locked")
		}

		// get the slice's len
		st.rwmutex.Lock()
		length := len(st.slice)
		st.rwmutex.Unlock()

		if length == 0 {
			// channel to wait for next enqueued element
			waitChan := make(chan T)

			select {
			// enqueue a watcher into the watchForNextElementChannel to wait for the next element
			case st.waitForNextElementChan <- waitChan:

				// n
				for {
					// re-checks every i milliseconds (top: 10 times) ... the following verifies if an item was enqueued
					// around the same time DequeueOrWaitForNextElementContext was invoked, meaning the waitChan wasn't yet sent over
					// st.waitForNextElementChan
					for i := 0; i < dequeueOrWaitForNextElementInvokeGapTime; i++ {
						select {
						case <-ctx.Done():
							return emptyRes, ctx.Err()
						case dequeuedItem := <-waitChan:
							return dequeuedItem, nil
						case <-time.After(time.Millisecond * time.Duration(i)):
							if dequeuedItem, err := st.Dequeue(); err == nil {
								return dequeuedItem, nil
							}
						}
					}

					// return the next enqueued element, if any
					select {
					// new enqueued element, no need to keep waiting
					case <-st.unlockDequeueOrWaitForNextElementChan:
						// check if we got a new element just after we got <-st.unlockDequeueOrWaitForNextElementChan
						select {
						case item := <-waitChan:
							return item, nil
						default:
						}
						// go back to: for loop
						continue

					case item := <-waitChan:
						return item, nil
					case <-ctx.Done():
						return emptyRes, ctx.Err()
					}
					// n
				}
			default:
				// too many watchers (waitForNextElementChanCapacity) enqueued waiting for next elements
				return emptyRes, NewQueueError(QueueErrorCodeEmptyQueue, "empty queue and can't wait for next element because there are too many DequeueOrWaitForNextElement() waiting")
			}
		}

		st.rwmutex.Lock()

		// verify that at least 1 item resides on the queue
		if len(st.slice) == 0 {
			st.rwmutex.Unlock()
			continue
		}
		elementToReturn := st.slice[0]
		st.slice = st.slice[1:]

		st.rwmutex.Unlock()
		return elementToReturn, nil
	}
}

// Get returns an element's value and keeps the element at the queue
func (st *TFIFO[T]) Get(index int) (T, error) {
	var emptyRes T
	if st.isLocked {
		return emptyRes, NewQueueError(QueueErrorCodeLockedQueue, "The queue is locked")
	}

	st.rwmutex.RLock()
	defer st.rwmutex.RUnlock()

	if len(st.slice) <= index {
		return emptyRes, NewQueueError(QueueErrorCodeIndexOutOfBounds, fmt.Sprintf("index out of bounds: %v", index))
	}

	return st.slice[index], nil
}

// Remove removes an element from the queue
func (st *TFIFO[T]) Remove(index int) error {
	if st.isLocked {
		return NewQueueError(QueueErrorCodeLockedQueue, "The queue is locked")
	}

	st.rwmutex.Lock()
	defer st.rwmutex.Unlock()

	if len(st.slice) <= index {
		return NewQueueError(QueueErrorCodeIndexOutOfBounds, fmt.Sprintf("index out of bounds: %v", index))
	}

	// remove the element
	st.slice = append(st.slice[:index], st.slice[index+1:]...)

	return nil
}

// GetLen returns the number of enqueued elements
func (st *TFIFO[T]) GetLen() int {
	st.rwmutex.RLock()
	defer st.rwmutex.RUnlock()

	return len(st.slice)
}

// GetCap returns the queue's capacity
func (st *TFIFO[T]) GetCap() int {
	st.rwmutex.RLock()
	defer st.rwmutex.RUnlock()

	return cap(st.slice)
}

// Lock // Locks the queue. No enqueue/dequeue operations will be allowed after this point.
func (st *TFIFO[T]) Lock() {
	st.lockRWmutex.Lock()
	defer st.lockRWmutex.Unlock()

	st.isLocked = true
}

// Unlock unlocks the queue
func (st *TFIFO[T]) Unlock() {
	st.lockRWmutex.Lock()
	defer st.lockRWmutex.Unlock()

	st.isLocked = false
}

// IsLocked returns true whether the queue is locked
func (st *TFIFO[T]) IsLocked() bool {
	st.lockRWmutex.RLock()
	defer st.lockRWmutex.RUnlock()

	return st.isLocked
}
