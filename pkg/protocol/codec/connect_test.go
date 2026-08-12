package codec

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/assert"
)

type connectV6Golden struct {
	Version                uint8  `json:"version"`
	DeviceFlag             uint8  `json:"device_flag"`
	DeviceID               string `json:"device_id"`
	UID                    string `json:"uid"`
	Token                  string `json:"token"`
	ClientTimestamp        int64  `json:"client_timestamp"`
	ClientKey              string `json:"client_key"`
	AppInstanceID          string `json:"app_instance_id"`
	InstallationGeneration uint64 `json:"installation_generation"`
	SessionGeneration      uint64 `json:"session_generation"`
	FrameHex               string `json:"frame_hex"`
}

func TestConnectV6Golden(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "connect_v6_golden.json"))
	assert.NoError(t, err)
	var fixture connectV6Golden
	assert.NoError(t, json.Unmarshal(raw, &fixture))

	packet := &frame.ConnectPacket{
		Version:                fixture.Version,
		DeviceFlag:             frame.DeviceFlag(fixture.DeviceFlag),
		DeviceID:               fixture.DeviceID,
		UID:                    fixture.UID,
		Token:                  fixture.Token,
		ClientTimestamp:        fixture.ClientTimestamp,
		ClientKey:              fixture.ClientKey,
		AppInstanceID:          fixture.AppInstanceID,
		InstallationGeneration: fixture.InstallationGeneration,
		SessionGeneration:      fixture.SessionGeneration,
	}
	wire, err := New().EncodeFrame(packet, frame.LatestVersion)
	assert.NoError(t, err)
	assert.Equal(t, fixture.FrameHex, hex.EncodeToString(wire))

	decoded, consumed, err := New().DecodeFrame(wire, frame.LatestVersion)
	assert.NoError(t, err)
	assert.Equal(t, len(wire), consumed)
	got := decoded.(*frame.ConnectPacket)
	assert.Equal(t, packet.Version, got.Version)
	assert.Equal(t, packet.DeviceFlag, got.DeviceFlag)
	assert.Equal(t, packet.DeviceID, got.DeviceID)
	assert.Equal(t, packet.UID, got.UID)
	assert.Equal(t, packet.Token, got.Token)
	assert.Equal(t, packet.ClientTimestamp, got.ClientTimestamp)
	assert.Equal(t, packet.ClientKey, got.ClientKey)
	assert.Equal(t, packet.AppInstanceID, got.AppInstanceID)
	assert.Equal(t, packet.InstallationGeneration, got.InstallationGeneration)
	assert.Equal(t, packet.SessionGeneration, got.SessionGeneration)
}

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
