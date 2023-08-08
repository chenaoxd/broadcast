package broadcast

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"
)

const (
	dropAmount = 64
)

var ErrBroadcastClosed = errors.New("broadcast closed")

type Broadcast[T any] interface {
	Subscribe() (<-chan T, error)
	Unsubscribe(ch <-chan T) error
	Close()
}

type broadcast[T any] struct {
	source     <-chan T
	listeners  []chan T
	addChan    chan chan T
	removeChan chan (<-chan T)
	closeChan  chan bool

	closed bool
	lisCap int
}

func NewBroadcast[T any](ctx context.Context, source <-chan T, lisCap int) Broadcast[T] {
	broadcast := &broadcast[T]{
		source:     source,
		listeners:  make([]chan T, 0),
		addChan:    make(chan chan T),
		removeChan: make(chan (<-chan T)),
		closeChan:  make(chan bool),
		closed:     false,
		lisCap:     lisCap,
	}
	go broadcast.serve(ctx)
	return broadcast
}

func (s *broadcast[T]) Close() {
	s.closeChan <- true
}

func (s *broadcast[T]) Subscribe() (<-chan T, error) {
	if s.closed {
		return nil, ErrBroadcastClosed
	}
	newListener := make(chan T, s.lisCap)
	logrus.Debugln("addChan <- new listender")
	s.addChan <- newListener
	return newListener, nil
}

func (s *broadcast[T]) Unsubscribe(ch <-chan T) error {
	if s.closed {
		return ErrBroadcastClosed
	}
	s.removeChan <- ch
	return nil
}

func (s *broadcast[T]) serve(ctx context.Context) { //nolint
	defer func() {
		s.closed = true
		for _, lis := range s.listeners {
			if lis != nil {
				close(lis)
			}
		}
	}()

	for {
		logrus.Debugln("broadcast ok...")
		select {
		case <-ctx.Done():
			logrus.Debugln("broadcast ctx done")
			return
		case <-s.closeChan:
			logrus.Debugln("chan closed")
			return
		case newLis := <-s.addChan:
			logrus.Debugln("lis <- addChan")
			s.listeners = append(s.listeners, newLis)
		case lisToRemove := <-s.removeChan:
			for i, ch := range s.listeners {
				if ch == lisToRemove {
					s.listeners[i] = s.listeners[len(s.listeners)-1]
					s.listeners = s.listeners[:len(s.listeners)-1]
					close(ch)
					break
				}
			}
		case val, ok := <-s.source:
			if !ok {
				logrus.Debugln("broadcast read from source not ok")
				return
			}
			for _, lis := range s.listeners {
				if lis != nil {
					// read from full lis
					// e.g. drop some log to make channel not blocking
					if len(lis) == s.lisCap {
						logrus.Debugf("lis chan is full, %d chans", len(s.listeners))
						for i := 0; i < dropAmount; i++ {
							select {
							case <-lis:
							default:
								logrus.Debugln("full lis empty")
								break
							}
						}
						logrus.Debugln("read from the full lis chan")
					}

					select {
					case lis <- val:
					case <-ctx.Done():
						logrus.Debugln("broadcast ctx done")
						return
					}
				}
			}
		}
	}
}
