package propose

import (
	"context"
	"errors"
	"fmt"
	"time"

	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
)

const (
	stageMetaCreateProposeLocal     = "meta_create_propose_local"
	stageMetaCreateProposeForward   = "meta_create_propose_forward"
	leaderChangeRetryBackoff        = 10 * time.Millisecond
	defaultLeaderChangeRetryTimeout = 5 * time.Second
)

// Config wires a Service.
type Config struct {
	// LocalNode is this node's stable cluster identity.
	LocalNode uint64
	// Router resolves request targets.
	Router Router
	// Slots proposes to local Slot leaders.
	Slots SlotRuntime
	// Forward forwards requests to remote leaders.
	Forward ForwardClient
	// LeaderChangeRetryTimeout bounds retries while Slot leadership is changing.
	// The production composition sets this from the configured maximum Slot
	// election window. A shorter caller context always wins.
	LeaderChangeRetryTimeout time.Duration
}

// Service routes Slot metadata proposals to local or remote leaders.
type Service struct {
	localNode uint64
	router    Router
	slots     SlotRuntime
	forward   ForwardClient
	// leaderChangeRetryTimeout is long enough for one configured Slot election.
	leaderChangeRetryTimeout time.Duration
}

// NewService creates a Service from cfg.
func NewService(cfg Config) *Service {
	retryTimeout := cfg.LeaderChangeRetryTimeout
	if retryTimeout <= 0 {
		retryTimeout = defaultLeaderChangeRetryTimeout
	}
	return &Service{
		localNode:                cfg.LocalNode,
		router:                   cfg.Router,
		slots:                    cfg.Slots,
		forward:                  cfg.Forward,
		leaderChangeRetryTimeout: retryTimeout,
	}
}

// Propose submits req to the current Slot leader.
func (s *Service) Propose(ctx context.Context, req Request) error {
	_, err := s.propose(ctx, req, false)
	return err
}

// ProposeResult submits req to the current Slot leader and returns apply bytes when supported.
func (s *Service) ProposeResult(ctx context.Context, req Request) ([]byte, error) {
	return s.propose(ctx, req, true)
}

func (s *Service) propose(ctx context.Context, req Request, wantResult bool) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	if s == nil || s.router == nil || s.slots == nil {
		return nil, ErrInvalidRequest
	}
	deadline := time.Now().Add(s.leaderChangeRetryTimeout)
	var lastErr error
	for {
		result, err := s.proposeOnceInternal(ctx, req, wantResult)
		if !isLeaderChangeRetryable(err) {
			return result, err
		}
		lastErr = err
		more, err := waitLeaderChangeRetry(ctx, deadline)
		if err != nil {
			return nil, err
		}
		if !more {
			return nil, lastErr
		}
	}
}

func (s *Service) proposeOnceInternal(ctx context.Context, req Request, wantResult bool) ([]byte, error) {
	route, err := s.route(req)
	if err != nil {
		return nil, err
	}
	payload := EncodePayload(route.HashSlot, req.Command)
	var lastRetryable error
	for _, leader := range routeLeaderCandidates(route) {
		var result []byte
		result, err = s.proposeToLeaderInternal(ctx, route, leader, payload, wantResult)
		if err == nil {
			return result, nil
		}
		if !isLeaderChangeRetryable(err) {
			return result, err
		}
		lastRetryable = err
	}
	if lastRetryable != nil {
		return nil, lastRetryable
	}
	return nil, ErrNotLeader
}

func (s *Service) proposeToLeaderInternal(ctx context.Context, route routing.Route, leader uint64, payload []byte, wantResult bool) ([]byte, error) {
	if leader == s.localNode || s.slots.IsLocalLeader(route.SlotID) {
		started := time.Now()
		var result []byte
		var err error
		if wantResult {
			if slots, ok := s.slots.(ResultSlotRuntime); ok {
				result, err = slots.ProposeResult(ctx, route.SlotID, payload)
			} else {
				err = s.slots.Propose(ctx, route.SlotID, payload)
			}
		} else {
			err = s.slots.Propose(ctx, route.SlotID, payload)
		}
		ObserveStage(ctx, stageMetaCreateProposeLocal, err, time.Since(started))
		return result, err
	}
	if s.forward == nil {
		return nil, fmt.Errorf("%w: missing forward client", ErrInvalidRequest)
	}
	started := time.Now()
	req := ForwardRequest{
		SlotID:     route.SlotID,
		HashSlot:   route.HashSlot,
		Class:      ProposalClassFromContext(ctx),
		WantResult: wantResult,
		Payload:    payload,
	}
	var result []byte
	var err error
	if wantResult {
		if forward, ok := s.forward.(ResultForwardClient); ok {
			result, err = forward.ForwardProposeResult(ctx, leader, req)
		} else {
			err = s.forward.ForwardPropose(ctx, leader, req)
		}
	} else {
		err = s.forward.ForwardPropose(ctx, leader, req)
	}
	ObserveStage(ctx, stageMetaCreateProposeForward, err, time.Since(started))
	return result, err
}

func routeLeaderCandidates(route routing.Route) []uint64 {
	candidates := make([]uint64, 0, 1+len(route.Peers))
	seen := make(map[uint64]struct{}, 1+len(route.Peers))
	add := func(nodeID uint64) {
		if nodeID == 0 {
			return
		}
		if _, ok := seen[nodeID]; ok {
			return
		}
		seen[nodeID] = struct{}{}
		candidates = append(candidates, nodeID)
	}
	add(route.Leader)
	for _, peer := range route.Peers {
		add(peer)
	}
	return candidates
}

func isLeaderChangeRetryable(err error) bool {
	return errors.Is(err, ErrNotLeader) || isDefinitelyNotDelivered(err)
}

// isDefinitelyNotDelivered recognizes only failures that occur before a
// request can enter a remote handler. Timeouts, closed connections, resets,
// queue failures, and other ambiguous transport outcomes must not be retried
// here because generic Slot commands are not all idempotent.
func isDefinitelyNotDelivered(err error) bool {
	return errors.Is(err, clusternet.ErrNodeNotFound) ||
		errors.Is(err, transport.ErrNodeNotFound) ||
		errors.Is(err, transport.ErrDialFailed)
}

func waitLeaderChangeRetry(ctx context.Context, deadline time.Time) (bool, error) {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false, nil
	}
	wait := min(leaderChangeRetryBackoff, remaining)
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-timer.C:
		return time.Now().Before(deadline), nil
	}
}

func (s *Service) route(req Request) (routing.Route, error) {
	if req.Target.HasSlotID {
		return s.router.RouteSlot(req.Target.SlotID, req.Target.HashSlot)
	}
	if req.Target.HasHashSlot {
		return s.router.RouteHashSlot(req.Target.HashSlot)
	}
	return s.router.RouteKey(req.Key)
}

func validateRequest(req Request) error {
	if len(req.Command) == 0 {
		return ErrInvalidRequest
	}
	if req.Target.HasSlotID && !req.Target.HasHashSlot {
		return ErrInvalidRequest
	}
	if !req.Target.HasHashSlot && req.Key == "" {
		return ErrInvalidRequest
	}
	return nil
}
