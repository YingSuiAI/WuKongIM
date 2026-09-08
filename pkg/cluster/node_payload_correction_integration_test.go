//go:build integration

package cluster

import (
	"bytes"
	"context"
	"hash/fnv"
	"io"
	"sync"
	"testing"
	"time"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/messagepayload"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/stretchr/testify/require"
)

func TestPayloadCorrectionReplicatedRestartKeepsOriginalLogAndClaim(t *testing.T) {
	nodes := newDefaultThreeNodeCluster(t)
	for _, node := range nodes {
		node.cfg.Slots.HashSlotCount = 256
	}
	startNodes(t, nodes...)
	t.Cleanup(func() { stopNodes(t, nodes...) })
	waitClusterReady(t, nodes...)
	waitNodeWriteReady(t, nodes[0])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id := ch.ChannelID{ID: "corrected-ordinary-message", Type: 2}
	original := []byte(`{"type":1,"content":"historical"}`)
	corrected := []byte(`{"type":1,"content":"canonical"}`)
	request := ch.AppendRequest{ChannelID: id, CommitMode: ch.CommitModeQuorum,
		Message: ch.Message{MessageID: 7201, FromUID: "human", ClientMsgNo: "original-send", Payload: original}}
	sent, err := nodes[0].AppendChannel(ctx, request)
	require.NoError(t, err)
	route := waitRouteKeyLeaderConverged(t, nodes, id.ID)
	writer := firstNonLeaderNode(t, nodes, route.Leader)
	correction := metadb.MessagePayloadCorrection{ChannelID: id.ID, ChannelType: int64(id.Type), MessageID: sent.MessageID,
		MessageSeq: sent.MessageSeq, FromUID: "human", ClientMsgNo: "original-send", OperationID: "opaque-operation",
		OriginalPayloadSHA256: payloadSHA256(original), CorrectedPayloadSHA256: payloadSHA256(corrected), CorrectedPayload: corrected}
	status, err := writer.CorrectMessagePayload(ctx, correction)
	require.NoError(t, err)
	require.Equal(t, metadb.PayloadCorrectionApplied, status)
	claim := CommittedMessageClaim{ChannelID: id.ID, ChannelType: id.Type, FromUID: "human", ClientMsgNo: "original-send", ExpectedOriginalPayloadSHA256: payloadSHA256(original)}
	verify := func() {
		t.Helper()
		for _, node := range nodes {
			raw, found, err := node.ReadChannelCommittedMessage(ctx, id, sent.MessageID, sent.MessageSeq)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, original, raw.Payload)
			require.Nil(t, raw.PayloadCorrection)
			current, err := node.ApplyMessagePayloadCorrections(ctx, []ch.Message{raw})
			require.NoError(t, err)
			require.Equal(t, corrected, current[0].Payload)
			require.Equal(t, original, raw.Payload, "projection must not mutate caller's original")
			require.Equal(t, correction.OperationID, current[0].PayloadCorrection.OperationID)
			proof, err := node.LookupCommittedMessageClaim(ctx, claim)
			require.NoError(t, err)
			require.NotNil(t, proof)
			require.Equal(t, corrected, proof.Payload)
			require.Equal(t, sent.MessageID, proof.MessageID)
			require.Equal(t, sent.MessageSeq, proof.MessageSeq)
			meta, err := node.GetChannelRuntimeMeta(ctx, id.ID, int64(id.Type))
			require.NoError(t, err)
			hit, found, err := nodes[meta.Leader-1].LookupChannelIdempotency(ctx, id, claim.FromUID, claim.ClientMsgNo)
			require.NoError(t, err)
			require.True(t, found)
			hash := fnv.New64a()
			_, _ = hash.Write(original)
			require.Equal(t, hash.Sum64(), hit.PayloadHash)
			require.Equal(t, original, hit.Message.Payload)
			head, err := node.ReadChannelCommittedHead(ctx, id)
			require.NoError(t, err)
			require.Equal(t, sent.MessageSeq, head)
		}
	}
	verify()
	status, err = writer.CorrectMessagePayload(ctx, correction)
	require.NoError(t, err)
	require.Equal(t, metadb.PayloadCorrectionReplayed, status)
	conflict := correction
	conflict.OperationID = "different-operation"
	status, err = writer.CorrectMessagePayload(ctx, conflict)
	require.NoError(t, err)
	require.Equal(t, metadb.PayloadCorrectionConflict, status)
	conflict = correction
	conflict.OriginalPayloadSHA256 = payloadSHA256([]byte("wrong"))
	_, err = writer.CorrectMessagePayload(ctx, conflict)
	require.ErrorIs(t, err, messagepayload.ErrConflict)
	missing := claim
	missing.ClientMsgNo = "never-sent"
	proof, err := writer.LookupCommittedMessageClaim(ctx, missing)
	require.NoError(t, err)
	require.Nil(t, proof)
	// Real Slot compaction and durable lifecycle preserve the raw SEND index.
	for _, node := range nodes {
		_, err := node.LocalCompactSlotRaftLog(ctx, route.SlotID)
		require.NoError(t, err)
	}
	stopNodes(t, nodes...)
	for i, node := range nodes {
		restarted, err := New(node.cfg)
		require.NoError(t, err)
		nodes[i] = restarted
	}
	startNodes(t, nodes...)
	require.NoError(t, WaitClusterReady(ctx, nodes...))
	waitRouteKeyLeaderConverged(t, nodes, id.ID)
	verify()
	meta, err := nodes[0].GetChannelRuntimeMeta(ctx, id.ID, int64(id.Type))
	require.NoError(t, err)
	require.NoError(t, nodes[0].AdvanceChannelRetentionThroughSeq(ctx, metadb.ChannelRetentionAdvance{
		ChannelID: id.ID, ChannelType: int64(id.Type), ExpectedChannelEpoch: meta.ChannelEpoch, ExpectedLeaderEpoch: meta.LeaderEpoch,
		ExpectedLeader: meta.Leader, ExpectedLeaseUntilMS: meta.LeaseUntilMS, RetentionThroughSeq: sent.MessageSeq, RetentionUpdatedAtMS: time.Now().UnixMilli()}))
	route = waitRouteKeyLeaderConverged(t, nodes, id.ID)
	_, found, err := nodes[route.Leader-1].defaultSlotMetaDB.ForHashSlot(route.HashSlot).GetMessagePayloadCorrection(ctx, correction.Key())
	require.NoError(t, err)
	require.False(t, found, "retention must delete the corrected body, not just hide original rows")
	// A page can have loaded raw rows before the retention commit. Decorating
	// that earlier page must not mistake purge for an uncorrected message.
	_, err = nodes[0].ApplyMessagePayloadCorrections(ctx, []ch.Message{{ChannelID: id.ID, ChannelType: id.Type, MessageID: sent.MessageID, MessageSeq: sent.MessageSeq, FromUID: claim.FromUID, ClientMsgNo: claim.ClientMsgNo, Payload: original}})
	require.ErrorIs(t, err, messagepayload.ErrNotFound)
	_, err = nodes[0].CorrectMessagePayload(ctx, correction)
	require.ErrorIs(t, err, messagepayload.ErrNotFound)
}

type payloadCorrectionApplyGate struct {
	started, proceed, applied chan struct{}
	once                      sync.Once
}

func (g *payloadCorrectionApplyGate) ObserveProposalStage(stage, _ string, _ time.Duration) {
	if stage == "meta_create_slot_raft_commit_wait" {
		g.once.Do(func() { close(g.started); <-g.proceed })
	}
	if stage == "meta_create_slot_fsm_apply" {
		close(g.applied)
	}
}

func TestPayloadCorrectionCanceledCallerCannotCrossActualRestorePartition(t *testing.T) {
	node := newDefaultSingleNode(t)
	node.cfg.Slots.HashSlotCount = 256
	startNode(t, node)
	t.Cleanup(func() { stopNodes(t, node) })
	waitNodeWriteReady(t, node)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	id := ch.ChannelID{ID: "correction-restore-owner", Type: 2}
	appendMessage := func(messageID uint64, client string, payload []byte) ch.AppendResult {
		result, err := node.AppendChannel(ctx, ch.AppendRequest{ChannelID: id, CommitMode: ch.CommitModeQuorum, Message: ch.Message{MessageID: messageID, FromUID: "human", ClientMsgNo: client, Payload: payload}})
		require.NoError(t, err)
		return result
	}
	_ = appendMessage(9101, "warm", []byte("warm"))
	route := waitRouteKeyLeaderReady(t, node, id.ID)
	// Capture an actual restorable partition at seq=1, before the target exists.
	node.setMaintenance(true)
	node.PauseLocalRestoreRuntime()
	metadata, err := node.OpenLocalRestoreMetadataSnapshot(ctx, route.HashSlot)
	require.NoError(t, err)
	metadataBytes, err := io.ReadAll(metadata)
	require.NoError(t, err)
	require.NoError(t, metadata.Close())
	messages, err := node.OpenLocalRestoreMessageSnapshot(ctx, route.HashSlot)
	require.NoError(t, err)
	messageBytes, err := io.ReadAll(messages.Reader)
	require.NoError(t, err)
	require.NoError(t, messages.Reader.Close())
	node.setMaintenance(false)
	node.ResumeLocalRestoreRuntime()
	original := []byte("old-target")
	sent := appendMessage(9102, "target", original)
	gate := &payloadCorrectionApplyGate{started: make(chan struct{}), proceed: make(chan struct{}), applied: make(chan struct{})}
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate.proceed) }) }
	t.Cleanup(releaseGate)
	requestCtx, requestCancel := context.WithCancel(multiraft.WithProposalStageObserver(ctx, gate))
	defer requestCancel()
	correction := metadb.MessagePayloadCorrection{ChannelID: id.ID, ChannelType: 2, MessageID: sent.MessageID, MessageSeq: sent.MessageSeq, FromUID: "human", ClientMsgNo: "target", OperationID: "restore-bound", OriginalPayloadSHA256: payloadSHA256(original), CorrectedPayloadSHA256: payloadSHA256([]byte("corrected")), CorrectedPayload: []byte("corrected")}
	done := make(chan error, 1)
	go func() { _, err := node.CorrectMessagePayload(requestCtx, correction); done <- err }()
	select {
	case <-gate.started:
	case <-ctx.Done():
		t.Fatal("real Slot did not reach committed/pre-apply gate")
	}
	requestCancel()
	require.ErrorIs(t, <-done, context.Canceled)
	maintenance := make(chan struct{})
	go func() { node.setMaintenance(true); node.PauseLocalRestoreRuntime(); close(maintenance) }()
	premature := false
	select {
	case <-maintenance:
		premature = true
	case <-time.After(50 * time.Millisecond):
	}
	install := func() {
		require.NoError(t, node.InstallLocalRestorePartition(ctx, route.HashSlot, bytes.NewReader(metadataBytes), int64(len(metadataBytes)), []RestoreMessageStream{{Reader: bytes.NewReader(messageBytes), Size: int64(len(messageBytes))}}))
	}
	if premature {
		install()
	}
	releaseGate()
	select {
	case <-gate.applied:
	case <-ctx.Done():
		t.Fatal("correction did not finish real FSM apply")
	}
	select {
	case <-maintenance:
	case <-ctx.Done():
		t.Fatal("maintenance did not acquire terminal proposal admission")
	}
	if !premature {
		install()
	}
	require.NoError(t, node.ActivateLocalRestore(ctx))
	node.setMaintenance(false)
	node.ResumeLocalRestoreRuntime()
	waitNodeWriteReady(t, node)
	fresh := appendMessage(9202, "post-restore", []byte("fresh-after-restore"))
	require.Equal(t, sent.MessageSeq, fresh.MessageSeq)
	raw, found, err := node.ReadChannelCommittedMessage(ctx, id, fresh.MessageID, fresh.MessageSeq)
	require.NoError(t, err)
	require.True(t, found)
	current, err := node.ApplyMessagePayloadCorrections(ctx, []ch.Message{raw})
	require.NoError(t, err, "a late old correction must not poison a newly restored/reused sequence")
	require.Equal(t, []byte("fresh-after-restore"), current[0].Payload)
	require.False(t, premature, "restore authority opened while accepted correction was still waiting for FSM apply")
}

func TestPayloadCorrectionCanceledBeforeProposalDoesNotWaitForMaintenanceLock(t *testing.T) {
	node := &Node{}
	node.maintenanceAdmissionMu.Lock()
	locked := true
	defer func() {
		if locked {
			node.maintenanceAdmissionMu.Unlock()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	payload := []byte("corrected")
	correction := metadb.MessagePayloadCorrection{ChannelID: "maintenance", ChannelType: 2, MessageID: 1, MessageSeq: 1, FromUID: "human", ClientMsgNo: "send", OperationID: "operation", OriginalPayloadSHA256: payloadSHA256([]byte("old")), CorrectedPayloadSHA256: payloadSHA256(payload), CorrectedPayload: payload}
	done := make(chan error, 1)
	go func() { _, err := node.createPayloadCorrectionLocal(ctx, correction); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		node.maintenanceAdmissionMu.Unlock()
		locked = false
		<-done
		t.Fatal("correction acquired outer admission before the proposal owner; nested readers can deadlock behind maintenance")
	}
}
