package proxy

import (
	"context"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

const authoritativeReadRPCAttemptTimeout = time.Second

const (
	rpcStatusOK        = "ok"
	rpcStatusNotLeader = "not_leader"
	rpcStatusNoLeader  = "no_leader"
	rpcStatusNoSlot    = "no_slot"
	rpcStatusNotFound  = "not_found"
	rpcStatusStaleMeta = "stale_meta"
)

type authoritativeRPCResponse interface {
	rpcStatus() string
	rpcLeaderID() uint64
}

type rpcStatusEncoder func(status string, leaderID uint64) ([]byte, error)

var defaultAuthoritativeRPCStatuses = map[string]struct{}{
	rpcStatusOK:       {},
	rpcStatusNotFound: {},
}

func (s *Store) shouldServeSlotLocally(slotID multiraft.SlotID) bool {
	if s == nil || s.cluster == nil {
		return false
	}
	return s.cluster.IsLocalSlotLeader(slotID)
}

func callAuthoritativeRPC[T authoritativeRPCResponse](
	ctx context.Context,
	s *Store,
	slotID multiraft.SlotID,
	serviceID uint8,
	payload []byte,
	decode func([]byte) (T, error),
) (T, error) {
	return callAuthoritativeRPCWithStatusesAndAttemptTimeout(ctx, s, slotID, serviceID, payload, decode, defaultAuthoritativeRPCStatuses, 0)
}

// callAuthoritativeReadRPC keeps an unavailable stale leader from consuming
// the entire foreground read deadline. Reads are safe to retry against the
// remaining Slot peers; mutation RPCs retain their existing outcome boundary.
func callAuthoritativeReadRPC[T authoritativeRPCResponse](
	ctx context.Context,
	s *Store,
	slotID multiraft.SlotID,
	serviceID uint8,
	payload []byte,
	decode func([]byte) (T, error),
) (T, error) {
	return callAuthoritativeRPCWithStatusesAndAttemptTimeout(ctx, s, slotID, serviceID, payload, decode, defaultAuthoritativeRPCStatuses, authoritativeReadRPCAttemptTimeout)
}

func callAuthoritativeRPCWithStatuses[T authoritativeRPCResponse](
	ctx context.Context,
	s *Store,
	slotID multiraft.SlotID,
	serviceID uint8,
	payload []byte,
	decode func([]byte) (T, error),
	acceptedStatuses map[string]struct{},
) (T, error) {
	return callAuthoritativeRPCWithStatusesAndAttemptTimeout(ctx, s, slotID, serviceID, payload, decode, acceptedStatuses, 0)
}

func callAuthoritativeRPCWithStatusesAndAttemptTimeout[T authoritativeRPCResponse](
	ctx context.Context,
	s *Store,
	slotID multiraft.SlotID,
	serviceID uint8,
	payload []byte,
	decode func([]byte) (T, error),
	acceptedStatuses map[string]struct{},
	attemptTimeout time.Duration,
) (T, error) {
	var zero T

	if s.cluster == nil {
		return zero, fmt.Errorf("metastore: cluster not configured")
	}

	peers := s.cluster.PeersForSlot(slotID)
	if len(peers) == 0 {
		return zero, errSlotNotFound
	}

	tried := make(map[multiraft.NodeID]struct{}, len(peers))
	candidates := append([]multiraft.NodeID(nil), peers...)
	// Prefer the observed leader so an unavailable earlier peer cannot consume
	// the request deadline before the authoritative node is attempted.
	if leaderID, err := s.cluster.LeaderOf(slotID); err == nil {
		for index, peer := range candidates {
			if peer == leaderID {
				candidates[0], candidates[index] = candidates[index], candidates[0]
				break
			}
		}
	}
	var lastErr error

	for len(candidates) > 0 {
		peer := candidates[0]
		candidates = candidates[1:]
		if _, ok := tried[peer]; ok {
			continue
		}
		tried[peer] = struct{}{}

		attemptCtx := ctx
		cancelAttempt := func() {}
		if attemptTimeout > 0 {
			attemptCtx, cancelAttempt = context.WithTimeout(ctx, attemptTimeout)
		}
		body, err := s.cluster.RPCService(attemptCtx, peer, slotID, serviceID, payload)
		cancelAttempt()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return zero, ctxErr
			}
			lastErr = err
			continue
		}

		resp, err := decode(body)
		if err != nil {
			lastErr = err
			continue
		}

		switch resp.rpcStatus() {
		case rpcStatusOK, rpcStatusNotFound, rpcStatusStaleMeta:
			if _, ok := acceptedStatuses[resp.rpcStatus()]; ok {
				return resp, nil
			}
			return zero, fmt.Errorf("metastore: unexpected rpc status %q", resp.rpcStatus())
		case rpcStatusNotLeader:
			if leaderID := multiraft.NodeID(resp.rpcLeaderID()); leaderID != 0 {
				if _, ok := tried[leaderID]; !ok {
					candidates = append([]multiraft.NodeID{leaderID}, candidates...)
				}
				continue
			}
		case rpcStatusNoLeader:
			lastErr = errNoLeader
			continue
		case rpcStatusNoSlot:
			lastErr = errSlotNotFound
			continue
		default:
			return zero, fmt.Errorf("metastore: unexpected rpc status %q", resp.rpcStatus())
		}
	}

	if lastErr != nil {
		return zero, lastErr
	}
	return zero, errNoLeader
}

func (s *Store) handleAuthoritativeRPC(slotID multiraft.SlotID, encode rpcStatusEncoder) ([]byte, bool, error) {
	if s.cluster.IsLocalSlotLeader(slotID) {
		return nil, false, nil
	}
	leaderID, err := s.cluster.LeaderOf(slotID)
	switch {
	case isSlotNotFound(err):
		body, encodeErr := encode(rpcStatusNoSlot, 0)
		return body, true, encodeErr
	case err != nil:
		body, encodeErr := encode(rpcStatusNoLeader, 0)
		return body, true, encodeErr
	case !s.cluster.IsLocal(leaderID):
		body, encodeErr := encode(rpcStatusNotLeader, uint64(leaderID))
		return body, true, encodeErr
	default:
		// The foreground router still names this node, but the local Raft
		// runtime does not. Do not serve stale local state or point the caller
		// back to this same non-leader node.
		body, encodeErr := encode(rpcStatusNoLeader, 0)
		return body, true, encodeErr
	}
}
