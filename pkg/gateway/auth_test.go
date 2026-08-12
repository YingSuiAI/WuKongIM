package gateway_test

import (
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkprotoenc"
)

func TestAuthenticatorRejectsProtocolBeforeV6(t *testing.T) {
	auth := gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{DisableEncryption: true})

	result, err := auth.Authenticate(nil, &frame.ConnectPacket{
		Version: 5,
		UID:     "u1",
	})
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if result.Connack.ReasonCode != frame.ReasonAuthFail || result.Connack.ServerVersion != frame.LatestVersion || result.SessionValues != nil {
		t.Fatalf("Authenticate(v5) = %#v, want explicit v6 rejection", result)
	}
}

func TestAuthenticatorStoresDeviceIDSessionValue(t *testing.T) {
	auth := gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{DisableEncryption: true})

	result, err := auth.Authenticate(nil, &frame.ConnectPacket{Version: frame.LatestVersion,
		UID:               "u1",
		DeviceID:          "d-1",
		AppInstanceID:     "app-1",
		SessionGeneration: 4,
	})
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if result.SessionValues[gateway.SessionValueDeviceID] != "d-1" {
		t.Fatalf("device id = %#v, want %q", result.SessionValues[gateway.SessionValueDeviceID], "d-1")
	}
	if result.SessionValues[gateway.SessionValueAppInstanceID] != "app-1" || result.SessionValues[gateway.SessionValueSessionGeneration] != uint64(4) {
		t.Fatalf("installation claims = %#v", result.SessionValues)
	}
}

func TestAuthenticatorBindsTokenVerificationToInstallationClaims(t *testing.T) {
	var got []any
	auth := gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{TokenAuthOn: true, DisableEncryption: true, VerifyToken: func(uid string, flag frame.DeviceFlag, deviceID, appInstanceID string, generation uint64, token string) (gateway.VerifiedCredential, error) {
		got = []any{uid, flag, deviceID, appInstanceID, generation, token}
		return gateway.VerifiedCredential{DeviceLevel: frame.DeviceLevelMaster, DeviceSessionID: "ds-1", IMSessionID: "im-1", InstallationGeneration: 2, AuthorizationFence: 9}, nil
	}})
	result, err := auth.Authenticate(nil, &frame.ConnectPacket{Version: frame.LatestVersion, UID: "u1", DeviceID: "d1", AppInstanceID: "a1", InstallationGeneration: 2, SessionGeneration: 3, DeviceFlag: frame.PC, Token: "token"})
	if err != nil || result.Connack.ReasonCode != frame.ReasonSuccess {
		t.Fatalf("Authenticate() = %#v, %v", result, err)
	}
	want := []any{"u1", frame.DeviceFlag(frame.PC), "d1", "a1", uint64(3), "token"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("claims = %#v, want %#v", got, want)
	}
	if result.SessionValues[gateway.SessionValueDeviceSessionID] != "ds-1" || result.SessionValues[gateway.SessionValueIMSessionID] != "im-1" || result.SessionValues[gateway.SessionValueInstallationGeneration] != uint64(2) || result.SessionValues[gateway.SessionValueAuthorizationFence] != uint64(9) {
		t.Fatalf("verified claims = %#v", result.SessionValues)
	}
}

func TestAuthenticatorNegotiatesWKProtoEncryption(t *testing.T) {
	auth := gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{
		EncryptionEnabled: true,
	})

	result, err := auth.Authenticate(nil, &frame.ConnectPacket{Version: frame.LatestVersion,
		UID:       "u1",
		ClientKey: testClientPublicKey(t),
	})
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if result.Connack.ServerKey == "" {
		t.Fatal("ServerKey is empty")
	}
	if result.Connack.Salt == "" {
		t.Fatal("Salt is empty")
	}
	if got := result.SessionValues[gateway.SessionValueEncryptionEnabled]; got != true {
		t.Fatalf("encryption enabled = %#v, want true", got)
	}
	if _, ok := result.SessionValues[gateway.SessionValueAESKey].([]byte); !ok {
		t.Fatalf("AESKey type = %T, want []byte", result.SessionValues[gateway.SessionValueAESKey])
	}
	if _, ok := result.SessionValues[gateway.SessionValueAESIV].([]byte); !ok {
		t.Fatalf("AESIV type = %T, want []byte", result.SessionValues[gateway.SessionValueAESIV])
	}
	if _, ok := result.SessionValues[gateway.SessionValueCrypto].(*wkprotoenc.SessionCrypto); !ok {
		t.Fatalf("SessionCrypto type = %T, want *wkprotoenc.SessionCrypto", result.SessionValues[gateway.SessionValueCrypto])
	}
}

func TestAuthenticatorRejectsMissingClientKeyWhenEncryptionEnabled(t *testing.T) {
	auth := gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{
		EncryptionEnabled: true,
	})

	result, err := auth.Authenticate(nil, &frame.ConnectPacket{Version: frame.LatestVersion, UID: "u1"})
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if got, want := result.Connack.ReasonCode, frame.ReasonClientKeyIsEmpty; got != want {
		t.Fatalf("ReasonCode = %v, want %v", got, want)
	}
}

func TestAuthenticatorRejectsInvalidClientKeyWhenEncryptionEnabled(t *testing.T) {
	auth := gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{
		EncryptionEnabled: true,
	})

	result, err := auth.Authenticate(nil, &frame.ConnectPacket{Version: frame.LatestVersion,
		UID:       "u1",
		ClientKey: "bad-client-key",
	})
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if got, want := result.Connack.ReasonCode, frame.ReasonAuthFail; got != want {
		t.Fatalf("ReasonCode = %v, want %v", got, want)
	}
}

func TestAuthenticatorSkipsEncryptionMaterialWhenDisabled(t *testing.T) {
	auth := gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{
		DisableEncryption: true,
	})

	result, err := auth.Authenticate(nil, &frame.ConnectPacket{Version: frame.LatestVersion,
		UID:       "u1",
		ClientKey: testClientPublicKey(t),
	})
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if result.Connack.ServerKey != "" {
		t.Fatalf("ServerKey = %q, want empty", result.Connack.ServerKey)
	}
	if result.Connack.Salt != "" {
		t.Fatalf("Salt = %q, want empty", result.Connack.Salt)
	}
	if got := result.SessionValues[gateway.SessionValueEncryptionEnabled]; got != nil {
		t.Fatalf("encryption enabled = %#v, want nil", got)
	}
}

func testClientPublicKey(t *testing.T) string {
	t.Helper()

	_, public, err := wkprotoenc.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair() error = %v", err)
	}
	return wkprotoenc.EncodePublicKey(public)
}
