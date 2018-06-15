// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4

import (
	"context"
	"net"
	"sync"
)

// NewXSub returns a new XSUB ZeroMQ socket.
// The returned socket value is initially unbound.
func NewXSub(ctx context.Context, opts ...Option) Socket {
	xsub := &xsubSocket{sck: newSocket(ctx, XSub, opts...)}
	xsub.sck.r = newQReader(xsub.sck.ctx)
	xsub.topics = make(map[string]struct{})
	return xsub
}

// xsubSocket is a XSUB ZeroMQ socket.
type xsubSocket struct {
	sck *socket

	mu     sync.RWMutex
	topics map[string]struct{}
}

// Close closes the open Socket
func (xsub *xsubSocket) Close() error {
	return xsub.sck.Close()
}

// Send puts the message on the outbound send queue.
// Send blocks until the message can be queued or the send deadline expires.
func (xsub *xsubSocket) Send(msg Msg) error {
	return xsub.sck.Send(msg)
}

// Recv receives a complete message.
func (xsub *xsubSocket) Recv() (Msg, error) {
	return xsub.sck.Recv()
}

// Listen connects a local endpoint to the Socket.
func (xsub *xsubSocket) Listen(ep string) error {
	return xsub.sck.Listen(ep)
}

// Dial connects a remote endpoint to the Socket.
func (xsub *xsubSocket) Dial(ep string) error {
	return xsub.sck.Dial(ep)
}

// Type returns the type of this Socket (PUB, SUB, ...)
func (xsub *xsubSocket) Type() SocketType {
	return xsub.sck.Type()
}

// Addr returns the listener's address.
// Addr returns nil if the socket isn't a listener.
func (xsub *xsubSocket) Addr() net.Addr {
	return xsub.sck.Addr()
}

// GetOption is used to retrieve an option for a socket.
func (xsub *xsubSocket) GetOption(name string) (interface{}, error) {
	return xsub.sck.GetOption(name)
}

// SetOption is used to set an option for a socket.
func (xsub *xsubSocket) SetOption(name string, value interface{}) error {
	if true && false {
		return xsub.sck.SetOption(name, value)
	}
	err := xsub.sck.SetOption(name, value)
	if err != nil {
		return err
	}

	var (
		topic []byte
	)

	switch name {
	case OptionSubscribe:
		k := value.(string)
		xsub.subscribe(k, 1)
		topic = append([]byte{1}, k...)

	case OptionUnsubscribe:
		k := value.(string)
		topic = append([]byte{0}, k...)
		xsub.subscribe(k, 0)

	default:
		return ErrBadProperty
	}

	xsub.sck.mu.RLock()
	if len(xsub.sck.conns) > 0 {
		err = xsub.Send(NewMsg(topic))
	}
	xsub.sck.mu.RUnlock()
	return err
}

func (xsub *xsubSocket) subscribe(topic string, v int) {
	xsub.mu.Lock()
	switch v {
	case 0:
		delete(xsub.topics, topic)
	case 1:
		xsub.topics[topic] = struct{}{}
	}
	xsub.mu.Unlock()
}

var (
	_ Socket = (*xsubSocket)(nil)
)
