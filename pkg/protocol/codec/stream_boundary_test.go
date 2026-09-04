package codec

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

func TestDecodeFrameTreatsEmptyInputAsIncomplete(t *testing.T) {
	proto := New()
	for _, input := range [][]byte{nil, {}} {
		got, consumed, err := proto.DecodeFrame(input, frame.LatestVersion)
		if err != nil || got != nil || consumed != 0 {
			t.Fatalf("DecodeFrame(%v) = (%T, %d, %v), want incomplete frame", input, got, consumed, err)
		}
	}
}

func TestProtocolRejectsUnsupportedFrameEncoding(t *testing.T) {
	proto := New()
	if data, err := proto.EncodeFrame(frame.Framer{FrameType: frame.UNKNOWN}, frame.LatestVersion); err == nil || len(data) != 0 {
		t.Fatalf("EncodeFrame(UNKNOWN) = (%v, %v), want explicit rejection", data, err)
	}
}
