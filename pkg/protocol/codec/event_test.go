package codec

import (
	"encoding/hex"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventEncodeAndDecode(t *testing.T) {
	packet := &frame.EventPacket{
		Id:        "123456",
		Type:      "test",
		Timestamp: 1234567890,
		Data:      []byte("test"),
	}
	codec := New()
	// 编码
	packetBytes, err := codec.EncodeFrame(packet, 1)
	assert.NoError(t, err)

	// 解码
	resultPacket, _, err := codec.DecodeFrame(packetBytes, 1)
	assert.NoError(t, err)
	resultEventPacket, ok := resultPacket.(*frame.EventPacket)
	assert.Equal(t, true, ok)

	// 比较
	assert.Equal(t, packet.Id, resultEventPacket.Id)
	assert.Equal(t, packet.Type, resultEventPacket.Type)
	assert.Equal(t, packet.Timestamp, resultEventPacket.Timestamp)
	assert.Equal(t, packet.Data, resultEventPacket.Data)
}

func TestEventV6Golden(t *testing.T) {
	want := "c01a000631323334353600047465737400000000499602d274657374"
	packet := &frame.EventPacket{
		Id:        "123456",
		Type:      "test",
		Timestamp: 1234567890,
		Data:      []byte("test"),
	}
	encoded, err := New().EncodeFrame(packet, frame.LatestVersion)
	require.NoError(t, err)
	assert.Equal(t, want, hex.EncodeToString(encoded))

	decoded, err := hex.DecodeString(want)
	require.NoError(t, err)
	got, consumed, err := New().DecodeFrame(decoded, frame.LatestVersion)
	require.NoError(t, err)
	assert.Equal(t, len(decoded), consumed)
	gotEvent, ok := got.(*frame.EventPacket)
	require.True(t, ok)
	assert.Equal(t, packet.Id, gotEvent.Id)
	assert.Equal(t, packet.Type, gotEvent.Type)
	assert.Equal(t, packet.Timestamp, gotEvent.Timestamp)
	assert.Equal(t, packet.Data, gotEvent.Data)
}
