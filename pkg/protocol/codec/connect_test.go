package codec

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/assert"
)

func TestConnectEncodeAndDecode(t *testing.T) {

	//clientTimestamp :=time.Now().UnixNano()/1000/1000
	packet := &frame.ConnectPacket{
		Version:                frame.LatestVersion,
		DeviceFlag:             1,
		DeviceID:               "deviceID",
		AppInstanceID:          "app-instance-1",
		InstallationGeneration: 3,
		SessionGeneration:      7,
		ClientTimestamp:        1,
		UID:                    "test",
	}

	codec := New()
	// 编码
	packetBytes, err := codec.EncodeFrame(packet, frame.LatestVersion)
	assert.NoError(t, err)
	// panic(fmt.Sprintf("%v",packetBytes))
	// 解码
	resultPacket, _, err := codec.DecodeFrame(packetBytes, frame.LatestVersion)
	assert.NoError(t, err)
	resultConnectPacket, ok := resultPacket.(*frame.ConnectPacket)
	assert.Equal(t, true, ok)

	assert.Equal(t, packet.Version, resultConnectPacket.Version)
	assert.Equal(t, packet.DeviceFlag, resultConnectPacket.DeviceFlag)
	assert.Equal(t, packet.DeviceID, resultConnectPacket.DeviceID)
	assert.Equal(t, packet.AppInstanceID, resultConnectPacket.AppInstanceID)
	assert.Equal(t, packet.InstallationGeneration, resultConnectPacket.InstallationGeneration)
	assert.Equal(t, packet.SessionGeneration, resultConnectPacket.SessionGeneration)
	assert.Equal(t, packet.ClientTimestamp, resultConnectPacket.ClientTimestamp)
	assert.Equal(t, packet.UID, resultConnectPacket.UID)
	assert.Equal(t, packet.Token, resultConnectPacket.Token)
}

func TestConnectBeforeV6DecodesOnlyVersionForExplicitRejection(t *testing.T) {
	packet := &frame.ConnectPacket{Version: frame.LegacyMessageSeqVersion, DeviceFlag: frame.PC, DeviceID: "old-device", UID: "old-client", Token: "old-token", ClientTimestamp: 123, ClientKey: "old-key"}
	wire, err := New().EncodeFrame(packet, frame.LegacyMessageSeqVersion)
	assert.NoError(t, err)
	decoded, consumed, err := New().DecodeFrame(wire, frame.LatestVersion)
	assert.NoError(t, err)
	assert.Equal(t, len(wire), consumed)
	connect := decoded.(*frame.ConnectPacket)
	assert.Equal(t, uint8(frame.LegacyMessageSeqVersion), connect.Version)
	// The decoder intentionally reads only the version. Gateway auth then rejects
	// the connection before any legacy identity can become a session.
	assert.Empty(t, connect.UID)
}
