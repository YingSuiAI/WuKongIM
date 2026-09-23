package gnet

import (
	"bytes"
	"testing"
)

func TestHandleWSTrafficPreservesQueuedPayloadAcrossReads(t *testing.T) {
	for _, tc := range []struct {
		name       string
		firstFrame []byte
		wantFirst  []string
	}{
		{
			name:       "one data frame",
			firstFrame: encodeMaskedTestWSFrame(t, true, wsOpcodeText, [4]byte{1, 2, 3, 4}, []byte("first")),
			wantFirst:  []string{"first"},
		},
		{
			name: "two data frames",
			firstFrame: append(
				encodeMaskedTestWSFrame(t, true, wsOpcodeText, [4]byte{1, 2, 3, 4}, []byte("first")),
				encodeMaskedTestWSFrame(t, true, wsOpcodeText, [4]byte{5, 6, 7, 8}, []byte("final"))...,
			),
			wantFirst: []string{"first", "final"},
		},
		{
			name: "data then control frame",
			firstFrame: append(
				encodeMaskedTestWSFrame(t, true, wsOpcodeText, [4]byte{1, 2, 3, 4}, []byte("first")),
				encodeMaskedTestWSFrame(t, true, wsOpcodePong, [4]byte{5, 6, 7, 8}, []byte("pong"))...,
			),
			wantFirst: []string{"first"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &allocTestGnetConn{next: tc.firstFrame}
			state := &connState{mode: connModeWSFrames}
			group := &engineGroup{}
			group.handleWSTraffic(conn, state)
			if len(state.queue) != len(tc.wantFirst) {
				t.Fatalf("queued events = %d, want %d", len(state.queue), len(tc.wantFirst))
			}

			conn.next = encodeMaskedTestWSFrame(t, true, wsOpcodeText, [4]byte{9, 10, 11, 12}, []byte("later"))
			group.handleWSTraffic(conn, state)
			if len(state.queue) != len(tc.wantFirst)+1 {
				t.Fatalf("queued events = %d, want %d", len(state.queue), len(tc.wantFirst)+1)
			}
			wantPending := len("later")
			for i, want := range tc.wantFirst {
				if got := state.queue[i].data; !bytes.Equal(got, []byte(want)) {
					t.Fatalf("queued payload %d = %q, want %q", i, got, want)
				}
				wantPending += len(want)
			}
			if got := state.queue[len(tc.wantFirst)].data; !bytes.Equal(got, []byte("later")) {
				t.Fatalf("second queued payload = %q, want later", got)
			}
			if got := state.pendingBytes; got != wantPending {
				t.Fatalf("pending bytes = %d, want %d", got, wantPending)
			}
		})
	}
}
