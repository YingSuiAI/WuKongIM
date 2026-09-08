package fsm

import (
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

func TestDeviceCredentialCommandClassification(t *testing.T) {
	device := EncodeUpsertDeviceCommand(metadb.Device{})
	if !IsDeviceCredentialCommand(device) {
		t.Fatal("device command not classified")
	}
	for _, data := range [][]byte{nil, {commandVersion}, {commandVersion + 1, cmdTypeUpsertDevice}, EncodeNoopCommand(), EncodeUpsertUserCommand(metadb.User{})} {
		if IsDeviceCredentialCommand(data) {
			t.Fatal("non-device command classified as credential mutation")
		}
	}
}
