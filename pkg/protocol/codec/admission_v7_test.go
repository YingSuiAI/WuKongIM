package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/require"
)

func TestAdmissionV7LengthPrefixesDoNotAllocate(t *testing.T) {
	buffer := bytes.NewBuffer(make([]byte, 0, 2))
	encoder := NewEncoderBuffer(buffer)
	allocations := testing.AllocsPerRun(100, func() { buffer.Reset(); encoder.WriteInt16(0xabcd) })
	require.Zero(t, allocations)
	require.Equal(t, []byte{0xab, 0xcd}, buffer.Bytes())
}

func TestAdmissionV7SendackGolden(t *testing.T) {
	packet := &frame.SendackPacket{
		MessageID: 2093294222990905344, ClientSeq: 17, MessageSeq: 9007199254740993,
		ReasonCode: frame.ReasonSuccess, ClientMsgNo: "client-message-0001",
		ApplicationMessageID: "019c0000-0000-7000-8000-000000000001",
	}
	encoded, err := New().EncodeFrame(packet, frame.ApplicationMessageIDVersion)
	require.NoError(t, err)
	// Construct the agreed binary layout independently of the production encoder.
	body := binary.BigEndian.AppendUint64(nil, uint64(packet.MessageID))
	body = binary.BigEndian.AppendUint32(body, 17)
	body = binary.BigEndian.AppendUint64(body, 9007199254740993)
	body = append(body, byte(frame.ReasonSuccess))
	for _, value := range []string{packet.ClientMsgNo, packet.ApplicationMessageID} {
		body = binary.BigEndian.AppendUint16(body, uint16(len(value)))
		body = append(body, value...)
	}
	expected := append([]byte{0x40, byte(len(body))}, body...)
	require.Equal(t, expected, encoded)
	decoded, n, err := New().DecodeFrame(encoded, frame.ApplicationMessageIDVersion)
	require.NoError(t, err)
	require.Equal(t, len(encoded), n)
	ack := decoded.(*frame.SendackPacket)
	require.Equal(t, packet.ApplicationMessageID, ack.ApplicationMessageID)
	require.Equal(t, packet.MessageSeq, ack.MessageSeq)
	t.Logf("v7-sendack-success=%s", hex.EncodeToString(encoded))
}

func TestAdmissionV7SendackRejectsMissingIdentityFieldAndTrailingBytes(t *testing.T) {
	packet := &frame.SendackPacket{ClientSeq: 17, ReasonCode: frame.ReasonAuthFail}
	encoded, err := New().EncodeFrame(packet, frame.ApplicationMessageIDVersion)
	require.NoError(t, err)
	t.Logf("v7-sendack-failure=%s", hex.EncodeToString(encoded))
	_, _, err = New().DecodeFrame(encoded, frame.ApplicationMessageIDVersion)
	require.NoError(t, err)
	for _, body := range [][]byte{encoded[2 : len(encoded)-1], append(append([]byte(nil), encoded[2:]...), 0)} {
		_, err := decodeSendack(frame.Framer{}, body, frame.ApplicationMessageIDVersion)
		require.Error(t, err)
	}
}

func TestAdmissionV7RecvKeepsV6Layout(t *testing.T) {
	packet := &frame.RecvPacket{
		MessageID: 2093294222990905344, MessageSeq: 9007199254740993,
		ClientMsgNo: "client-message-0001", FromUID: "sender", ChannelID: "room", ChannelType: 2,
		Timestamp: 1788364800, Payload: []byte(`{"id":"019c0000-0000-7000-8000-000000000001","type":"message.committed"}`),
	}
	v6, err := New().EncodeFrame(packet, frame.MessageSeqU64Version)
	require.NoError(t, err)
	v7, err := New().EncodeFrame(packet, frame.ApplicationMessageIDVersion)
	require.NoError(t, err)
	require.Equal(t, v6, v7)
	t.Logf("v7-recv-layout=%s", hex.EncodeToString(v7))
}
