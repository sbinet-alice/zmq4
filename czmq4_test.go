// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build czmq4

package zmq4_test

import (
	"context"

	"github.com/go-zeromq/zmq4"
)

var (
	cpushpulls = []testCasePushPull{
		{
			name:     "tcp-cpush-pull",
			endpoint: must(EndPoint("tcp")),
			push:     zmq4.NewCPush(bkg),
			pull:     zmq4.NewPull(bkg),
		},
		{
			name:     "tcp-push-cpull",
			endpoint: must(EndPoint("tcp")),
			push:     zmq4.NewPush(bkg),
			pull:     zmq4.NewCPull(bkg),
		},
		{
			name:     "tcp-cpush-cpull",
			endpoint: must(EndPoint("tcp")),
			push:     zmq4.NewCPush(bkg),
			pull:     zmq4.NewCPull(bkg),
		},
		{
			name:     "ipc-cpush-pull",
			endpoint: "ipc://ipc-cpush-pull",
			push:     zmq4.NewCPush(bkg),
			pull:     zmq4.NewPull(bkg),
		},
		{
			name:     "ipc-push-cpull",
			endpoint: "ipc://ipc-push-cpull",
			push:     zmq4.NewPush(bkg),
			pull:     zmq4.NewCPull(bkg),
		},
		{
			name:     "ipc-cpush-cpull",
			endpoint: "ipc://ipc-cpush-cpull",
			push:     zmq4.NewCPush(bkg),
			pull:     zmq4.NewCPull(bkg),
		},
		//{
		//	name:     "udp-cpush-cpull",
		//	endpoint: "udp://127.0.0.1:55555",
		//	push:     zmq4.NewCPush(),
		//	pull:     zmq4.NewCPull(),
		//},
		{
			name:     "inproc-cpush-cpull",
			endpoint: "inproc://cpush-cpull",
			push:     zmq4.NewCPush(bkg),
			pull:     zmq4.NewCPull(bkg),
		},
	}

	creqreps = []testCaseReqRep{
		{
			name:     "tcp-creq-rep",
			endpoint: must(EndPoint("tcp")),
			req:      zmq4.NewCReq(bkg),
			rep:      zmq4.NewRep(bkg),
		},
		{
			name:     "tcp-req-crep",
			endpoint: must(EndPoint("tcp")),
			req:      zmq4.NewReq(bkg),
			rep:      zmq4.NewCRep(bkg),
		},
		{
			name:     "tcp-creq-crep",
			endpoint: must(EndPoint("tcp")),
			req:      zmq4.NewCReq(bkg),
			rep:      zmq4.NewCRep(bkg),
		},
		{
			name:     "ipc-creq-rep",
			endpoint: "ipc://ipc-creq-rep",
			req:      zmq4.NewCReq(bkg),
			rep:      zmq4.NewRep(bkg),
		},
		{
			name:     "ipc-req-crep",
			endpoint: "ipc://ipc-req-crep",
			req:      zmq4.NewReq(bkg),
			rep:      zmq4.NewCRep(bkg),
		},
		{
			name:     "ipc-creq-crep",
			endpoint: "ipc://ipc-creq-crep",
			req:      zmq4.NewCReq(bkg),
			rep:      zmq4.NewCRep(bkg),
		},
		{
			name:     "inproc-creq-crep",
			endpoint: "inproc://inproc-creq-crep",
			req:      zmq4.NewCReq(bkg),
			rep:      zmq4.NewCRep(bkg),
		},
	}

	cpubsubs = []testCasePubSub{
		{
			name:     "tcp-cpub-sub",
			endpoint: must(EndPoint("tcp")),
			pub:      zmq4.NewCPub(bkg),
			sub0:     zmq4.NewSub(bkg),
			sub1:     zmq4.NewSub(bkg),
			sub2:     zmq4.NewSub(bkg),
		},
		{
			name:     "tcp-pub-csub",
			endpoint: must(EndPoint("tcp")),
			pub:      zmq4.NewPub(bkg),
			sub0:     zmq4.NewCSub(bkg),
			sub1:     zmq4.NewCSub(bkg),
			sub2:     zmq4.NewCSub(bkg),
		},
		{
			name:     "tcp-cpub-csub",
			endpoint: must(EndPoint("tcp")),
			pub:      zmq4.NewCPub(bkg),
			sub0:     zmq4.NewCSub(bkg),
			sub1:     zmq4.NewCSub(bkg),
			sub2:     zmq4.NewCSub(bkg),
		},
		{
			name:     "ipc-cpub-sub",
			endpoint: "ipc://ipc-cpub-sub",
			pub:      zmq4.NewCPub(bkg),
			sub0:     zmq4.NewSub(bkg),
			sub1:     zmq4.NewSub(bkg),
			sub2:     zmq4.NewSub(bkg),
		},
		{
			name:     "ipc-pub-csub",
			endpoint: "ipc://ipc-pub-csub",
			pub:      zmq4.NewPub(bkg),
			sub0:     zmq4.NewCSub(bkg),
			sub1:     zmq4.NewCSub(bkg),
			sub2:     zmq4.NewCSub(bkg),
		},
		{
			name:     "ipc-cpub-csub",
			endpoint: "ipc://ipc-cpub-csub",
			pub:      zmq4.NewCPub(bkg),
			sub0:     zmq4.NewCSub(bkg),
			sub1:     zmq4.NewCSub(bkg),
			sub2:     zmq4.NewCSub(bkg),
		},
		{
			name:     "inproc-cpub-csub",
			endpoint: "inproc://inproc-cpub-csub",
			pub:      zmq4.NewCPub(bkg),
			sub0:     zmq4.NewCSub(bkg),
			sub1:     zmq4.NewCSub(bkg),
			sub2:     zmq4.NewCSub(bkg),
		},
	}

	crouterdealers = []testCaseRouterDealer{
		{
			name:     "tcp-router-cdealer",
			skip:     true,
			endpoint: func() string { return must(EndPoint("tcp")) },
			router: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewRouter(ctx, zmq4.WithID(zmq4.SocketIdentity("router")))
			},
			dealer0: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-0")))
			},
			dealer1: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-1")))
			},
			dealer2: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-2")))
			},
		},
		{
			name:     "tcp-crouter-dealer",
			endpoint: func() string { return must(EndPoint("tcp")) },
			router: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCRouter(ctx, zmq4.CWithID(zmq4.SocketIdentity("router")))
			},
			dealer0: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewDealer(ctx, zmq4.WithID(zmq4.SocketIdentity("dealer-0")))
			},
			dealer1: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewDealer(ctx, zmq4.WithID(zmq4.SocketIdentity("dealer-1")))
			},
			dealer2: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewDealer(ctx, zmq4.WithID(zmq4.SocketIdentity("dealer-2")))
			},
		},
		{
			name:     "tcp-crouter-cdealer",
			endpoint: func() string { return must(EndPoint("tcp")) },
			router: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCRouter(ctx, zmq4.CWithID(zmq4.SocketIdentity("router")))
			},
			dealer0: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-0")))
			},
			dealer1: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-1")))
			},
			dealer2: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-2")))
			},
		},
		{
			name:     "ipc-router-cdealer",
			skip:     true,
			endpoint: func() string { return "ipc://ipc-router-cdealer" },
			router: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewRouter(ctx, zmq4.WithID(zmq4.SocketIdentity("router")))
			},
			dealer0: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-0")))
			},
			dealer1: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-1")))
			},
			dealer2: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-2")))
			},
		},
		{
			name:     "ipc-crouter-dealer",
			endpoint: func() string { return "ipc://crouter-dealer" },
			router: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCRouter(ctx, zmq4.CWithID(zmq4.SocketIdentity("router")))
			},
			dealer0: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewDealer(ctx, zmq4.WithID(zmq4.SocketIdentity("dealer-0")))
			},
			dealer1: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewDealer(ctx, zmq4.WithID(zmq4.SocketIdentity("dealer-1")))
			},
			dealer2: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewDealer(ctx, zmq4.WithID(zmq4.SocketIdentity("dealer-2")))
			},
		},
		{
			name:     "ipc-crouter-cdealer",
			endpoint: func() string { return "ipc://crouter-cdealer" },
			router: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCRouter(ctx, zmq4.CWithID(zmq4.SocketIdentity("router")))
			},
			dealer0: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-0")))
			},
			dealer1: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-1")))
			},
			dealer2: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-2")))
			},
		},
		{
			name:     "inproc-crouter-cdealer",
			endpoint: func() string { return "inproc://crouter-cdealer" },
			router: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCRouter(ctx, zmq4.CWithID(zmq4.SocketIdentity("router")))
			},
			dealer0: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-0")))
			},
			dealer1: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-1")))
			},
			dealer2: func(ctx context.Context) zmq4.Socket {
				return zmq4.NewCDealer(ctx, zmq4.CWithID(zmq4.SocketIdentity("dealer-2")))
			},
		},
	}

	cxpubxsubs = []testCaseXPubXSub{
		{
			name:     "tcp-cxpub-xsub",
			endpoint: must(EndPoint("tcp")),
			xpub:     zmq4.NewCXPub(bkg),
			xsub0:    zmq4.NewXSub(bkg),
			xsub1:    zmq4.NewXSub(bkg),
			xsub2:    zmq4.NewXSub(bkg),
		},
		{
			name:     "tcp-xpub-cxsub",
			endpoint: must(EndPoint("tcp")),
			xpub:     zmq4.NewXPub(bkg),
			xsub0:    zmq4.NewCXSub(bkg),
			xsub1:    zmq4.NewCXSub(bkg),
			xsub2:    zmq4.NewCXSub(bkg),
		},
		{
			name:     "tcp-cxpub-cxsub",
			endpoint: must(EndPoint("tcp")),
			xpub:     zmq4.NewCXPub(bkg),
			xsub0:    zmq4.NewCXSub(bkg),
			xsub1:    zmq4.NewCXSub(bkg),
			xsub2:    zmq4.NewCXSub(bkg),
		},
		{
			name:     "ipc-cxpub-xsub",
			endpoint: "ipc://ipc-cxpub-xsub",
			xpub:     zmq4.NewCXPub(bkg),
			xsub0:    zmq4.NewXSub(bkg),
			xsub1:    zmq4.NewXSub(bkg),
			xsub2:    zmq4.NewXSub(bkg),
		},
		{
			name:     "ipc-xpub-cxsub",
			endpoint: "ipc://ipc-xpub-cxsub",
			xpub:     zmq4.NewXPub(bkg),
			xsub0:    zmq4.NewCXSub(bkg),
			xsub1:    zmq4.NewCXSub(bkg),
			xsub2:    zmq4.NewCXSub(bkg),
		},
		{
			name:     "ipc-cxpub-cxsub",
			endpoint: "ipc://ipc-cxpub-cxsub",
			xpub:     zmq4.NewCXPub(bkg),
			xsub0:    zmq4.NewCXSub(bkg),
			xsub1:    zmq4.NewCXSub(bkg),
			xsub2:    zmq4.NewCXSub(bkg),
		},
	}
)

func init() {
	pushpulls = append(pushpulls, cpushpulls...)
	reqreps = append(reqreps, creqreps...)
	pubsubs = append(pubsubs, cpubsubs...)
	routerdealers = append(routerdealers, crouterdealers...)
	xpubxsubs = append(xpubxsubs, cxpubxsubs...)
}
