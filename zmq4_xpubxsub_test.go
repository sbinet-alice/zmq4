// Copyright 2018 The go-zeromq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zmq4_test

import (
	"context"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

var (
	xpubxsubs = []testCaseXPubXSub{
		{
			name:     "tcp-xpub-xsub",
			endpoint: must(EndPoint("tcp")),
			xpub:     zmq4.NewPub(bkg),
			xsub0:    zmq4.NewSub(bkg, zmq4.WithID(zmq4.SocketIdentity("xsub0"))),
			xsub1:    zmq4.NewSub(bkg, zmq4.WithID(zmq4.SocketIdentity("xsub1"))),
			xsub2:    zmq4.NewSub(bkg),
		},
		{
			name:     "ipc-xpub-xsub",
			endpoint: "ipc://ipc-xpub-xsub",
			xpub:     zmq4.NewPub(bkg),
			xsub0:    zmq4.NewSub(bkg, zmq4.WithID(zmq4.SocketIdentity("xsub0"))),
			xsub1:    zmq4.NewSub(bkg, zmq4.WithID(zmq4.SocketIdentity("xsub1"))),
			xsub2:    zmq4.NewSub(bkg),
		},
	}
)

type testCaseXPubXSub struct {
	name     string
	skip     bool
	endpoint string
	xpub     zmq4.Socket
	xsub0    zmq4.Socket
	xsub1    zmq4.Socket
	xsub2    zmq4.Socket
}

func TestXPubXSub(t *testing.T) {
	var (
		topics      = []string{"", "MSG", "msg"}
		wantNumMsgs = []int{3, 1, 1}
		msg0        = zmq4.NewMsgString("anything")
		msg1        = zmq4.NewMsgString("MSG 1")
		msg2        = zmq4.NewMsgString("msg 2")
		msgs        = [][]zmq4.Msg{
			0: {msg0, msg1, msg2},
			1: {msg1},
			2: {msg2},
		}
	)

	for _, tc := range xpubxsubs {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf(tc.name)
			}

			// FIXME(sbinet): we should probably do this at the zmq4.Socket.Close level
			if strings.HasPrefix(tc.endpoint, "ipc://") {
				defer os.Remove(tc.endpoint[len("ipc://"):])
			}

			ep := tc.endpoint

			ctx, timeout := context.WithTimeout(context.Background(), 20*time.Second)
			defer timeout()

			defer tc.xpub.Close()
			defer tc.xsub0.Close()
			defer tc.xsub1.Close()
			defer tc.xsub2.Close()

			nmsgs := []int{0, 0, 0}
			xsubs := []zmq4.Socket{tc.xsub0, tc.xsub1, tc.xsub2}

			var wg1 sync.WaitGroup
			var wg2 sync.WaitGroup
			wg1.Add(len(xsubs))
			wg2.Add(len(xsubs))

			grp, ctx := errgroup.WithContext(ctx)
			grp.Go(func() error {

				err := tc.xpub.Listen(ep)
				if err != nil {
					return xerrors.Errorf("could not listen: %w", err)
				}

				if addr := tc.xpub.Addr(); addr == nil {
					return xerrors.Errorf("listener with nil Addr")
				}

				wg1.Wait()
				wg2.Wait()

				time.Sleep(1 * time.Second)

				for _, msg := range msgs[0] {
					err = tc.xpub.Send(msg)
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

						err = xsub.SetOption(zmq4.OptionSubscribe, topics[isub])
						if err != nil {
							return xerrors.Errorf("could not subscribe to topic %q: %w", topics[isub], err)
						}

						wg2.Done()
						wg2.Wait()

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

			for i, want := range wantNumMsgs {
				if want != nmsgs[i] {
					t.Errorf("sub[%d]: got %d messages, want %d msgs=%v", i, nmsgs[i], want, nmsgs)
				}
			}
		})
	}
}
