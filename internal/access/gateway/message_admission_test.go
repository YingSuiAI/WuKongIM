package gateway

import (
	"testing"

	coregateway "github.com/WuKongIM/WuKongIM/pkg/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/require"
)

func TestSendAdmissionIdentityComesFromVerifiedSession(t *testing.T) {
	sess := newTestSession(t, nil)
	sess.SetValue(coregateway.SessionValueUID, "authenticated-sender")
	sess.SetValue(coregateway.SessionValueIMSessionID, "019c0000-0000-7000-8000-000000000001")
	packet := &frame.SendPacket{ChannelID: "room", ChannelType: 2, ClientMsgNo: "client-message-0001",
		Payload: []byte(`{"from_uid":"other-user","im_session_id":"forged","origin":"service","id":"forged"}`)}
	command, err := mapSendCommand(&coregateway.Context{Session: sess}, packet)
	require.NoError(t, err)
	require.Equal(t, "authenticated-sender", command.FromUID)
	require.Equal(t, "019c0000-0000-7000-8000-000000000001", command.IMSessionID)
	require.Equal(t, sess.ID(), command.SenderSessionID)
	require.False(t, command.ServiceAuthenticated)
	require.False(t, command.ApplicationAdmission)
}
