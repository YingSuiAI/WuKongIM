package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/messagepayload"
	"github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

// Sixteen maximum-sized corrections keep each metadata response bounded below
// six MiB after JSON/base64 encoding, independent of the history page size.
const payloadCorrectionReadBatch = 16

type payloadCorrectionRPCRequest struct {
	Version    int                                  `json:"version"`
	Op         string                               `json:"op"`
	Correction *metadb.MessagePayloadCorrection     `json:"correction,omitempty"`
	Keys       []metadb.MessagePayloadCorrectionKey `json:"keys,omitempty"`
	Claim      *CommittedMessageClaim               `json:"claim,omitempty"`
}

type payloadCorrectionRPCResponse struct {
	Version     int                               `json:"version"`
	Error       string                            `json:"error,omitempty"`
	Status      metadb.PayloadCorrectionStatus    `json:"status,omitempty"`
	Corrections []metadb.MessagePayloadCorrection `json:"corrections,omitempty"`
	Message     *ch.Message                       `json:"message,omitempty"`
}

type messagePayloadCorrectionRPCHandler struct{ node *Node }

func (h messagePayloadCorrectionRPCHandler) HandleRPC(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) > 512*1024 {
		return nil, metadb.ErrInvalidArgument
	}
	var request payloadCorrectionRPCRequest
	if json.Unmarshal(payload, &request) != nil || request.Version != 1 {
		return nil, metadb.ErrInvalidArgument
	}
	response := payloadCorrectionRPCResponse{Version: 1}
	var err error
	switch request.Op {
	case "create":
		if request.Correction == nil {
			return nil, metadb.ErrInvalidArgument
		}
		response.Status, err = h.node.createPayloadCorrectionLocal(ctx, *request.Correction)
	case "read":
		response.Corrections, err = h.node.readPayloadCorrectionsLocal(ctx, request.Keys)
	case "claim":
		if request.Claim == nil {
			return nil, metadb.ErrInvalidArgument
		}
		response.Message, err = h.node.lookupCommittedMessageClaimLocal(ctx, *request.Claim)
	default:
		return nil, metadb.ErrInvalidArgument
	}
	if errors.Is(err, messagepayload.ErrConflict) {
		response.Error = "conflict"
	} else if errors.Is(err, messagepayload.ErrNotFound) {
		response.Error = "not_found"
	} else if err != nil {
		return nil, err
	}
	return json.Marshal(response)
}

func (n *Node) callPayloadCorrection(ctx context.Context, nodeID uint64, request payloadCorrectionRPCRequest) (payloadCorrectionRPCResponse, error) {
	request.Version = 1
	encoded, err := json.Marshal(request)
	if err != nil {
		return payloadCorrectionRPCResponse{}, err
	}
	data, err := n.CallRPC(ctx, nodeID, clusternet.RPCMessagePayloadCorrection, encoded)
	if err != nil {
		return payloadCorrectionRPCResponse{}, err
	}
	var response payloadCorrectionRPCResponse
	if json.Unmarshal(data, &response) != nil || response.Version != 1 {
		return response, metadb.ErrCorruptValue
	}
	switch response.Error {
	case "":
		return response, nil
	case "conflict":
		return response, messagepayload.ErrConflict
	case "not_found":
		return response, messagepayload.ErrNotFound
	default:
		return response, metadb.ErrCorruptValue
	}
}

// CorrectMessagePayload verifies the immutable original through the Channel
// Leader, then serializes one create-only projection through its owning Slot.
// It never enters SEND or changes the original Channel log/index.
func (n *Node) CorrectMessagePayload(ctx context.Context, correction metadb.MessagePayloadCorrection) (metadb.PayloadCorrectionStatus, error) {
	if err := n.ensureForeground(); err != nil {
		return 0, err
	}
	if err := correction.Validate(); err != nil {
		return 0, err
	}
	route, err := n.RouteKey(correction.ChannelID)
	if err != nil {
		return 0, err
	}
	if route.Leader == 0 {
		return 0, ErrNoSlotLeader
	}
	if route.Leader != n.NodeID() {
		response, err := n.callPayloadCorrection(ctx, route.Leader, payloadCorrectionRPCRequest{Op: "create", Correction: &correction})
		if err == nil && (response.Status < metadb.PayloadCorrectionApplied || response.Status > metadb.PayloadCorrectionNotFound) {
			err = metadb.ErrCorruptValue
		}
		return response.Status, err
	}
	return n.createPayloadCorrectionLocal(ctx, correction)
}

func (n *Node) createPayloadCorrectionLocal(ctx context.Context, correction metadb.MessagePayloadCorrection) (metadb.PayloadCorrectionStatus, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	if err := correction.Validate(); err != nil {
		return 0, err
	}
	// One admission spans original proof through Slot commitment so restore
	// cannot replace the proven base in between. Enqueue below uses the runtime
	// directly while already admitted, never recursively acquiring this RWMutex.
	release, err := n.acquireWriteAdmission()
	if err != nil {
		return 0, err
	}
	callerOwnsAdmission := true
	defer func() {
		if callerOwnsAdmission {
			release()
		}
	}()
	route, err := n.payloadCorrectionSlotLeaderRoute(ctx, correction.ChannelID)
	if err != nil {
		return 0, err
	}
	message, found, err := n.ReadChannelCommittedMessage(ctx, ch.ChannelID{ID: correction.ChannelID, Type: uint8(correction.ChannelType)}, correction.MessageID, correction.MessageSeq)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, messagepayload.ErrNotFound
	}
	if message.FromUID != correction.FromUID || message.ClientMsgNo != correction.ClientMsgNo || payloadSHA256(message.Payload) != correction.OriginalPayloadSHA256 {
		return 0, messagepayload.ErrConflict
	}
	command, err := fsm.EncodeMessagePayloadCorrectionCommand(correction)
	if err != nil {
		return 0, err
	}
	future, err := n.defaultSlotRuntime.ProposeObserved(ctx, multiraft.SlotID(route.SlotID), multiraftPayload(route.HashSlot, command), payloadCorrectionAdmissionRelease{release: release})
	if err != nil {
		return 0, mapMultiraftProposeError(err)
	}
	callerOwnsAdmission = false
	result, err := future.Wait(ctx)
	if err != nil {
		return 0, mapMultiraftProposeError(err)
	}
	return fsm.DecodeMessagePayloadCorrectionResult(result.Data)
}

type payloadCorrectionAdmissionRelease struct{ release func() }

func (r payloadCorrectionAdmissionRelease) ObserveFutureCompletion(multiraft.Result, error) {
	r.release()
}

// ApplyMessagePayloadCorrections is the single current-body projection seam.
// Callers pass raw committed rows. Replication, backup, original-claim proofs
// and SEND idempotency do not call this method.
func (n *Node) ApplyMessagePayloadCorrections(ctx context.Context, messages []ch.Message) ([]ch.Message, error) {
	if len(messages) == 0 {
		return messages, nil
	}
	if err := n.ensureForeground(); err != nil {
		return nil, err
	}
	out := append([]ch.Message(nil), messages...)
	for start := 0; start < len(out); start += payloadCorrectionReadBatch {
		end := min(start+payloadCorrectionReadBatch, len(out))
		groups := make(map[uint64][]metadb.MessagePayloadCorrectionKey)
		for _, message := range out[start:end] {
			route, err := n.RouteKey(message.ChannelID)
			if err != nil {
				return nil, err
			}
			if route.Leader == 0 {
				return nil, ErrNoSlotLeader
			}
			groups[route.Leader] = append(groups[route.Leader], metadb.MessagePayloadCorrectionKey{ChannelID: message.ChannelID, ChannelType: int64(message.ChannelType), MessageSeq: message.MessageSeq})
		}
		corrections := make(map[metadb.MessagePayloadCorrectionKey]metadb.MessagePayloadCorrection)
		for leader, keys := range groups {
			var rows []metadb.MessagePayloadCorrection
			var err error
			if leader == n.NodeID() {
				rows, err = n.readPayloadCorrectionsLocal(ctx, keys)
			} else {
				var response payloadCorrectionRPCResponse
				response, err = n.callPayloadCorrection(ctx, leader, payloadCorrectionRPCRequest{Op: "read", Keys: keys})
				rows = response.Corrections
			}
			if err != nil {
				return nil, err
			}
			requested := make(map[metadb.MessagePayloadCorrectionKey]bool, len(keys))
			for _, key := range keys {
				requested[key] = true
			}
			for _, row := range rows {
				if !requested[row.Key()] || row.Validate() != nil {
					return nil, metadb.ErrCorruptValue
				}
				if prior, exists := corrections[row.Key()]; exists && !reflect.DeepEqual(prior, row) {
					return nil, metadb.ErrCorruptValue
				}
				corrections[row.Key()] = row
			}
		}
		for index := start; index < end; index++ {
			message := &out[index]
			row, exists := corrections[metadb.MessagePayloadCorrectionKey{ChannelID: message.ChannelID, ChannelType: int64(message.ChannelType), MessageSeq: message.MessageSeq}]
			if !exists {
				continue
			}
			if message.MessageID != row.MessageID || message.FromUID != row.FromUID || message.ClientMsgNo != row.ClientMsgNo ||
				payloadSHA256(message.Payload) != row.OriginalPayloadSHA256 {
				return nil, messagepayload.ErrConflict
			}
			message.Payload = append([]byte(nil), row.CorrectedPayload...)
			message.PayloadCorrection = &messagepayload.Proof{OperationID: row.OperationID,
				OriginalPayloadSHA256: row.OriginalPayloadSHA256, CorrectedPayloadSHA256: row.CorrectedPayloadSHA256}
		}
	}
	return out, nil
}

func (n *Node) readPayloadCorrectionsLocal(ctx context.Context, keys []metadb.MessagePayloadCorrectionKey) ([]metadb.MessagePayloadCorrection, error) {
	if len(keys) == 0 || len(keys) > payloadCorrectionReadBatch {
		return nil, metadb.ErrInvalidArgument
	}
	if err := n.ensureForeground(); err != nil {
		return nil, err
	}
	rows := make([]metadb.MessagePayloadCorrection, 0, len(keys))
	barriers := make(map[uint32]multiraft.ReadBarrierResult)
	for _, key := range keys {
		route, err := n.payloadCorrectionSlotLeaderRoute(ctx, key.ChannelID)
		if err != nil {
			return nil, err
		}
		barrier, found := barriers[route.SlotID]
		if !found {
			barrier, err = n.SlotReadBarrier(ctx, multiraft.SlotID(route.SlotID))
			if err != nil {
				return nil, err
			}
			barriers[route.SlotID] = barrier
		}
		if barrier.Term != route.LeaderTerm || uint64(barrier.LeaderID) != route.Leader {
			return nil, ErrNotLeader
		}
		row, found, err := n.defaultSlotMetaDB.ForHashSlot(route.HashSlot).ReadCurrentPayloadCorrection(ctx, key)
		if errors.Is(err, metadb.ErrNotFound) {
			return nil, messagepayload.ErrNotFound
		}
		if err != nil {
			return nil, err
		}
		if found {
			rows = append(rows, row)
		}
		current, err := n.payloadCorrectionSlotLeaderRoute(ctx, key.ChannelID)
		if err != nil {
			return nil, err
		}
		if route.HashSlot != current.HashSlot || route.SlotID != current.SlotID || route.Leader != current.Leader || route.LeaderTerm != current.LeaderTerm || route.ConfigEpoch != current.ConfigEpoch {
			return nil, ErrNotLeader
		}
	}
	return rows, nil
}

func (n *Node) payloadCorrectionSlotLeaderRoute(ctx context.Context, channelID string) (Route, error) {
	if err := ctxErr(ctx); err != nil {
		return Route{}, err
	}
	route, err := n.RouteKey(channelID)
	if err != nil {
		return Route{}, err
	}
	if n.defaultSlotRuntime == nil || n.defaultSlotMetaDB == nil {
		return Route{}, ErrNotStarted
	}
	status, err := n.defaultSlotRuntime.FreshStatus(ctx, multiraft.SlotID(route.SlotID))
	if err != nil {
		return Route{}, err
	}
	if err := payloadCorrectionReadAuthority(status, n.NodeID()); err != nil {
		return Route{}, err
	}
	route.Leader = uint64(status.LeaderID)
	route.LeaderTerm = status.Term
	return route, nil
}

func payloadCorrectionReadAuthority(status multiraft.Status, nodeID uint64) error {
	if status.Role != multiraft.RoleLeader || uint64(status.LeaderID) != nodeID {
		return ErrNotLeader
	}
	if status.AppliedIndex < status.CommitIndex {
		return messagepayload.ErrUnavailable
	}
	return nil
}

func payloadSHA256(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

// CommittedMessageClaim is an exact original SEND claim, not permission to send.
type CommittedMessageClaim struct {
	ChannelID                     string `json:"channel_id"`
	ChannelType                   uint8  `json:"channel_type"`
	FromUID                       string `json:"from_uid"`
	ClientMsgNo                   string `json:"client_msg_no"`
	ExpectedOriginalPayloadSHA256 string `json:"expected_original_payload_sha256"`
}

// LookupCommittedMessageClaim routes the existing durable index to its current
// Channel Leader, then proves the hit is committed. Absence never appends data.
func (n *Node) LookupCommittedMessageClaim(ctx context.Context, claim CommittedMessageClaim) (*ch.Message, error) {
	if err := n.ensureForeground(); err != nil {
		return nil, err
	}
	if !validCommittedClaim(claim) {
		return nil, metadb.ErrInvalidArgument
	}
	meta, err := n.GetChannelRuntimeMeta(ctx, claim.ChannelID, int64(claim.ChannelType))
	if errors.Is(err, metadb.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if meta.Leader == 0 {
		return nil, ErrNoSlotLeader
	}
	if meta.Leader != n.NodeID() {
		response, err := n.callPayloadCorrection(ctx, meta.Leader, payloadCorrectionRPCRequest{Op: "claim", Claim: &claim})
		if err == nil && response.Message != nil && !claimMatchesCurrentMessage(claim, *response.Message) {
			err = metadb.ErrCorruptValue
		}
		return response.Message, err
	}
	return n.lookupCommittedMessageClaimLocal(ctx, claim)
}

func (n *Node) lookupCommittedMessageClaimLocal(ctx context.Context, claim CommittedMessageClaim) (*ch.Message, error) {
	if !validCommittedClaim(claim) {
		return nil, metadb.ErrInvalidArgument
	}
	meta, err := n.GetChannelRuntimeMeta(ctx, claim.ChannelID, int64(claim.ChannelType))
	if err != nil {
		return nil, err
	}
	if meta.Leader != n.NodeID() {
		return nil, ErrNotLeader
	}
	id := ch.ChannelID{ID: claim.ChannelID, Type: claim.ChannelType}
	hit, found, err := n.LookupChannelIdempotency(ctx, id, claim.FromUID, claim.ClientMsgNo)
	if err != nil {
		return nil, err
	}
	var message ch.Message
	if found {
		message, found, err = n.ReadChannelCommittedMessage(ctx, id, hit.Message.MessageID, hit.Message.MessageSeq)
	} else {
		_, err = n.ReadChannelCommittedHead(ctx, id)
	}
	if err != nil {
		return nil, err
	}
	current, err := n.GetChannelRuntimeMeta(ctx, claim.ChannelID, int64(claim.ChannelType))
	if err != nil {
		return nil, err
	}
	if current.Leader != meta.Leader || current.ChannelEpoch != meta.ChannelEpoch || current.LeaderEpoch != meta.LeaderEpoch {
		return nil, ch.ErrStaleMeta
	}
	if !found {
		return nil, nil
	}
	if !claimMatchesMessage(claim, message) || payloadSHA256(message.Payload) != claim.ExpectedOriginalPayloadSHA256 {
		return nil, messagepayload.ErrConflict
	}
	effective, err := n.ApplyMessagePayloadCorrections(ctx, []ch.Message{message})
	if errors.Is(err, messagepayload.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &effective[0], nil
}

func claimMatchesMessage(claim CommittedMessageClaim, message ch.Message) bool {
	return message.MessageID > 0 && message.MessageSeq > 0 && message.ChannelID == claim.ChannelID &&
		message.ChannelType == claim.ChannelType && message.FromUID == claim.FromUID && message.ClientMsgNo == claim.ClientMsgNo
}

func validCommittedClaim(claim CommittedMessageClaim) bool {
	return claim.ChannelID != "" && len(claim.ChannelID) <= 512 && claim.ChannelType != 0 &&
		claim.FromUID != "" && len(claim.FromUID) <= 512 && claim.ClientMsgNo != "" && len(claim.ClientMsgNo) <= 512 &&
		metadb.ValidPayloadSHA256(claim.ExpectedOriginalPayloadSHA256)
}

func claimMatchesCurrentMessage(claim CommittedMessageClaim, message ch.Message) bool {
	if !claimMatchesMessage(claim, message) {
		return false
	}
	if proof := message.PayloadCorrection; proof != nil {
		return proof.OperationID != "" && len(proof.OperationID) <= 256 && proof.OriginalPayloadSHA256 == claim.ExpectedOriginalPayloadSHA256 &&
			proof.CorrectedPayloadSHA256 == payloadSHA256(message.Payload)
	}
	return payloadSHA256(message.Payload) == claim.ExpectedOriginalPayloadSHA256
}
