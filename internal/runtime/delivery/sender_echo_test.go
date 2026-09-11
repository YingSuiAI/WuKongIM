package delivery

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/contracts/onlinedelivery"
	"github.com/stretchr/testify/require"
)

func TestCanonicalSenderEchoUsesNormalDurableAckOwnership(t *testing.T) {
	route := runtimeRouteForTest()
	var writes []LocalSessionWrite
	runtime := NewRuntime(RuntimeOptions{
		LocalNodeID: 1, EchoSender: true,
		Presence: planPresenceResolverFunc(func(context.Context, []onlinedelivery.RecipientTargetBatch) []TargetPresenceResult {
			return []TargetPresenceResult{{Routes: []onlinedelivery.Route{route}}}
		}),
		SessionWriter: localSessionWriterFunc(func(_ context.Context, write LocalSessionWrite) SessionWriteResult {
			writes = append(writes, write)
			return SessionWriteResult{Disposition: SessionWriteAccepted}
		}),
	})
	startRuntimeForTest(t, runtime)
	plan := runtimePlanForTest(42)
	plan.Event.FromUID = route.UID
	plan.Event.SenderNodeID = route.OwnerNodeID
	plan.Event.SenderSessionID = route.SessionID
	plan.Event.ClientMsgNo = "client-message-0001"
	plan.Event.Payload = []byte("canonical admitted body")
	require.NoError(t, runtime.processPlan(context.Background(), plan))
	require.Len(t, writes, 1)
	require.Equal(t, plan.Event, writes[0].Event)
	require.Equal(t, route, writes[0].Route)
	require.Equal(t, 1, runtime.PendingAckCount())
	// Redelivery follows the same exact-session ACK ownership; it creates no alternate sender path.
	require.NoError(t, runtime.processPlan(context.Background(), plan))
	require.Len(t, writes, 2)
	require.Equal(t, 1, runtime.PendingAckCount())
	require.NoError(t, runtime.Recvack(context.Background(), Recvack{UID: route.UID, SessionID: route.SessionID, MessageID: 42, MessageSeq: 42}))
	require.Zero(t, runtime.PendingAckCount())
}
