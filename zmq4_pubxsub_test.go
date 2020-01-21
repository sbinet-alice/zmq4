// Copyright 2020 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

var (
	pubxsubs = []testCasePubXSub{
		{
			name:     "tcp-pub-xsub",
			endpoint: must(EndPoint("tcp")),
			pub:      zmq4.NewXPub(bkg),
			xsub0:    zmq4.NewXSub(bkg, zmq4.WithID(zmq4.SocketIdentity("sub0"))),
			xsub1:    zmq4.NewXSub(bkg, zmq4.WithID(zmq4.SocketIdentity("sub1"))),
			xsub2:    zmq4.NewXSub(bkg, zmq4.WithID(zmq4.SocketIdentity("sub2"))),
		},
		{
			name:     "ipc-pub-xsub",
			endpoint: "ipc://ipc-pub-xsub",
			pub:      zmq4.NewPub(bkg),
			xsub0:    zmq4.NewXSub(bkg, zmq4.WithID(zmq4.SocketIdentity("sub0"))),
			xsub1:    zmq4.NewXSub(bkg, zmq4.WithID(zmq4.SocketIdentity("sub1"))),
			xsub2:    zmq4.NewXSub(bkg, zmq4.WithID(zmq4.SocketIdentity("sub2"))),
		},
		{
			name:     "inproc-pub-xsub",
			endpoint: "inproc://inproc-pub-xsub",
			pub:      zmq4.NewPub(bkg),
			xsub0:    zmq4.NewXSub(bkg, zmq4.WithID(zmq4.SocketIdentity("sub0"))),
			xsub1:    zmq4.NewXSub(bkg, zmq4.WithID(zmq4.SocketIdentity("sub1"))),
			xsub2:    zmq4.NewXSub(bkg, zmq4.WithID(zmq4.SocketIdentity("sub2"))),
		},
	}
)

type testCasePubXSub struct {
	name     string
	skip     bool
	endpoint string
	pub      zmq4.Socket
	xsub0    zmq4.Socket
	xsub1    zmq4.Socket
	xsub2    zmq4.Socket
}

func TestPubXSub(t *testing.T) {

	var (
		wantNumMsgs = []int{3, 1, 1}
		msg0        = zmq4.NewMsgString("msg 0")
		msg1        = zmq4.NewMsgString("MSG 1")
		msg2        = zmq4.NewMsgString("msg 2")
		msgs        = [][]zmq4.Msg{
			{msg0, msg1, msg2},
			{msg0, msg1, msg2},
			{msg0, msg1, msg2},
		}
	)

	for i := range pubxsubs {
		tc := pubxsubs[i]
		t.Run(tc.name, func(t *testing.T) {
			defer tc.pub.Close()
			defer tc.xsub0.Close()
			defer tc.xsub1.Close()
			defer tc.xsub2.Close()

			ep := tc.endpoint
			cleanUp(ep)

			if tc.skip {
				t.Skipf(tc.name)
			}
			t.Parallel()

			ctx, timeout := context.WithTimeout(context.Background(), 20*time.Second)
			defer timeout()

			nmsgs := []int{0, 0, 0}
			xsubs := []zmq4.Socket{tc.xsub0, tc.xsub1, tc.xsub2}

			var wg1 sync.WaitGroup
			wg1.Add(len(xsubs))

			grp, ctx := errgroup.WithContext(ctx)
			grp.Go(func() error {

				err := tc.pub.Listen(ep)
				if err != nil {
					return xerrors.Errorf("could not listen: %w", err)
				}

				if addr := tc.pub.Addr(); addr == nil {
					return xerrors.Errorf("listener with nil Addr")
				}

				wg1.Wait()

				time.Sleep(1 * time.Second)

				for _, msg := range msgs[0] {
					err = tc.pub.Send(msg)
					if err != nil {
						return xerrors.Errorf("could not send message %v: %w", msg, err)
					}
				}

				return err
			})

			for isub := range xsubs {
				func(isub int, xsub zmq4.Socket) {
					grp.Go(func() error {
						var err error
						err = xsub.Dial(ep)
						if err != nil {
							return xerrors.Errorf("could not dial: %w", err)
						}

						if addr := xsub.Addr(); addr != nil {
							return xerrors.Errorf("dialer with non-nil Addr")
						}

						wg1.Done()
						wg1.Wait()

						msgs := msgs[isub]
						for imsg, want := range msgs {
							msg, err := xsub.Recv()
							if err != nil {
								return xerrors.Errorf("could not recv message %v: %w", want, err)
							}
							if !reflect.DeepEqual(msg, want) {
								return xerrors.Errorf("sub[%d][msg=%d]: got = %v, want= %v", isub, imsg, msg, want)
							}
							nmsgs[isub]++
						}

						return err
					})
				}(isub, xsubs[isub])
			}

			if err := grp.Wait(); err != nil {
				t.Fatalf("error: %+v", err)
			}

			if err := ctx.Err(); err != nil && err != context.Canceled {
				t.Fatalf("error: %+v", err)
			}

			for i, want := range wantNumMsgs {
				if want != nmsgs[i] {
					t.Errorf("xsub[%d]: got %d messages, want %d msgs=%v", i, nmsgs[i], want, nmsgs)
				}
			}
		})
	}
}
