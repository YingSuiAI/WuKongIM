package codec

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/require"
)

type eventReducerGolden struct {
	EventTypes []string `json:"event_types"`
	Cases      []struct {
		Name      string          `json:"name"`
		EventID   string          `json:"event_id"`
		FrameType string          `json:"frame_type"`
		Timestamp uint64          `json:"timestamp"`
		Data      json.RawMessage `json:"data"`
		Previous  uint64          `json:"previous_sequence"`
		Expect    struct {
			Action            string `json:"action"`
			WatermarkKey      string `json:"watermark_key"`
			ReducerKey        string `json:"reducer_key"`
			Sequence          uint64 `json:"sequence"`
			AuthoritySequence uint64 `json:"authority_sequence"`
			RunTerminal       bool   `json:"run_terminal"`
		} `json:"expect"`
	} `json:"cases"`
}

func TestEventV6ReducerGolden(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "event_v6_reducer_golden.json"))
	require.NoError(t, err)
	var fixture eventReducerGolden
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.NotEmpty(t, fixture.Cases)
	require.Equal(t, []string{"open", "delta", "snapshot", "finish"}, fixture.EventTypes)

	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			packet := &frame.EventPacket{Id: tc.EventID, Type: tc.FrameType, Timestamp: tc.Timestamp, Data: tc.Data}
			encoded, err := New().EncodeFrame(packet, frame.LatestVersion)
			require.NoError(t, err)
			decoded, consumed, err := New().DecodeFrame(encoded, frame.LatestVersion)
			require.NoError(t, err)
			require.Equal(t, len(encoded), consumed)
			got := decoded.(*frame.EventPacket)
			require.Equal(t, packet.Id, got.Id)
			require.Equal(t, packet.Type, got.Type)
			require.Equal(t, packet.Timestamp, got.Timestamp)

			var envelope struct {
				MessageID   uint64 `json:"message_id"`
				MsgEventSeq uint64 `json:"msg_event_seq"`
				Payload     struct {
					RunID             string `json:"run_id"`
					EventType         string `json:"event_type"`
					EventKey          string `json:"event_key"`
					AuthoritySequence uint64 `json:"authority_sequence"`
				} `json:"payload"`
			}
			require.NoError(t, json.Unmarshal(got.Data, &envelope))
			require.Equal(t, got.Type, envelope.Payload.EventType)
			require.Equal(t, tc.Expect.ReducerKey, envelope.Payload.RunID+":"+envelope.Payload.EventKey)
			require.Equal(t, tc.Expect.WatermarkKey, fmt.Sprintf("%d:%s", envelope.MessageID, envelope.Payload.RunID))
			require.Equal(t, tc.Expect.Sequence, envelope.MsgEventSeq)
			require.Equal(t, tc.Expect.AuthoritySequence, envelope.Payload.AuthoritySequence)
			require.NotContains(t, envelope.Payload.EventKey, envelope.Payload.RunID+":")
		})
	}
}
