package channels

import (
	"context"
	"errors"
	"fmt"
	"time"

	ch "github.com/WuKongIM/WuKongIM/pkg/channel"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/propose"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/routing"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
)

const (
	channelMetaStageSlotRead      = "meta_slot_read"
	channelMetaStageCreateBuild   = "meta_create_build"
	channelMetaStageCreatePropose = "meta_create_propose"
	channelMetaStageCreateWrite   = "meta_create_write"
	channelMetaStageFinalRead     = "meta_final_read"

	// One caller-owned retry window spans the configured Slot election timeout.
	// Each iteration rereads the create-only key before another identical
	// ChannelID/ChannelType admission is submitted.
	channelMetaLeaderChangeRetryTimeout = 4 * time.Second
	channelMetaLeaderChangeRetryBackoff = 25 * time.Millisecond
)

// SlotMetaSource resolves Channel metadata from Slot authoritative runtime metadata.
type SlotMetaSource struct {
	reader   RuntimeMetaReader
	batcher  *metaCreateBatcher
	batchErr error
	opts     SlotMetaSourceOptions
}

// NewSlotMetaSource creates a Slot-backed ChannelMetaSource.
func NewSlotMetaSource(reader RuntimeMetaReader, opts ...SlotMetaSourceOptions) *SlotMetaSource {
	cfg := SlotMetaSourceOptions{}
	if len(opts) > 0 {
		cfg = opts[0]
	}
	cfg.DefaultReplicas = append([]ch.NodeID(nil), cfg.DefaultReplicas...)
	source := &SlotMetaSource{reader: reader, opts: cfg}
	if cfg.Router != nil && cfg.BatchStore != nil {
		source.batcher = newMetaCreateBatcher(cfg.Router, cfg.BatchStore, cfg.BatchObserver, cfg.Goroutines, source.buildRuntimeMetaBatch, source.observeMetaStage)
		if cfg.metaCreateCollectWait > 0 {
			source.batcher.collectWait = cfg.metaCreateCollectWait
		}
	} else if cfg.Router != nil || cfg.BatchStore != nil {
		source.batchErr = fmt.Errorf("%w: runtime metadata batch router/store must be configured together", ch.ErrInvalidConfig)
	}
	return source
}

// Close stops new metadata-create admission, cancels queued entries, and joins
// the bounded in-flight Slot batches owned by this source.
func (s *SlotMetaSource) Close() error {
	if s == nil || s.batcher == nil {
		return nil
	}
	return s.batcher.close()
}

// ResolveChannelMeta returns metadata for id from authoritative Slot storage.
func (s *SlotMetaSource) ResolveChannelMeta(ctx context.Context, id ch.ChannelID) (ch.Meta, error) {
	if err := ctxErr(ctx); err != nil {
		return ch.Meta{}, err
	}
	started := time.Now()
	meta, err := s.readRuntimeMeta(ctx, id)
	s.observeMetaStage(channelMetaStageSlotRead, metaStageResult(err), time.Since(started))
	if err != nil {
		if errors.Is(err, metadb.ErrNotFound) {
			return ch.Meta{}, fmt.Errorf("%w: %v", ch.ErrChannelNotFound, id)
		}
		return ch.Meta{}, err
	}
	return projectRuntimeMeta(meta), nil
}

// ResolveChannelMetas batch-reads Slot-owned runtime metadata while preserving
// input alignment and item-scoped missing or stale outcomes.
func (s *SlotMetaSource) ResolveChannelMetas(ctx context.Context, ids []ch.ChannelID) []ChannelMetaResult {
	resolved := make([]ChannelMetaResult, len(ids))
	if len(ids) == 0 {
		return resolved
	}
	reader, ok := s.reader.(RuntimeMetaBatchReader)
	if !ok {
		for i, id := range ids {
			meta, err := s.ResolveChannelMeta(ctx, id)
			resolved[i] = ChannelMetaResult{Meta: meta, Found: err == nil, Err: err}
		}
		return resolved
	}
	keys := make([]metadb.ChannelKey, len(ids))
	for i, id := range ids {
		keys[i] = metadb.ChannelKey{ChannelID: id.ID, ChannelType: int64(id.Type)}
	}
	started := time.Now()
	readResults, err := reader.BatchReadChannelRuntimeMetas(ctx, keys)
	s.observeMetaStage(channelMetaStageSlotRead, metaStageResult(err), time.Since(started))
	if err != nil {
		for i := range resolved {
			resolved[i].Err = err
		}
		return resolved
	}
	if len(readResults) != len(ids) {
		err := fmt.Errorf("%w: aligned runtime metadata batch", ch.ErrInvalidConfig)
		aligned := make([]ChannelMetaResult, len(ids))
		for i := range aligned {
			aligned[i].Err = err
		}
		return aligned
	}
	for i, result := range readResults {
		if result.Err != nil {
			if errors.Is(result.Err, metadb.ErrNotFound) {
				resolved[i].Err = fmt.Errorf("%w: %v", ch.ErrChannelNotFound, ids[i])
			} else {
				resolved[i].Err = result.Err
			}
			continue
		}
		if result.Meta.ChannelID != ids[i].ID || result.Meta.ChannelType != int64(ids[i].Type) {
			resolved[i].Err = fmt.Errorf("%w: resolved %s/%d for %v", ch.ErrStaleMeta, result.Meta.ChannelID, result.Meta.ChannelType, ids[i])
			continue
		}
		resolved[i] = ChannelMetaResult{Meta: projectRuntimeMeta(result.Meta), Found: true}
	}
	return resolved
}

// EnsureChannelMeta returns metadata for append admission, creating it when absent.
func (s *SlotMetaSource) EnsureChannelMeta(ctx context.Context, id ch.ChannelID) (ch.Meta, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ch.Meta{}, err
	}
	retryCtx, cancel := context.WithTimeout(ctx, channelMetaLeaderChangeRetryTimeout)
	defer cancel()

	var lastRetryErr error
	for {
		started := time.Now()
		meta, err := s.readRuntimeMeta(retryCtx, id)
		s.observeMetaStage(channelMetaStageSlotRead, metaStageResult(err), time.Since(started))
		if err == nil {
			return projectRuntimeMeta(meta), nil
		}
		if !errors.Is(err, metadb.ErrNotFound) {
			if !retryableChannelMetaLeaderChange(err) {
				return ch.Meta{}, err
			}
			lastRetryErr = err
		} else {
			if s.batchErr != nil {
				return ch.Meta{}, s.batchErr
			}
			if s.batcher == nil {
				return ch.Meta{}, fmt.Errorf("%w: missing runtime metadata batch router/store", ch.ErrInvalidConfig)
			}
			started = time.Now()
			outcome := s.batcher.ensure(retryCtx, id)
			meta, err = outcome.meta, outcome.err
			s.observeMetaStage(channelMetaStageCreateWrite, metaStageResult(err), time.Since(started))
			if err == nil {
				return projectRuntimeMeta(meta), nil
			}
			if !retryableChannelMetaLeaderChange(err) {
				return ch.Meta{}, err
			}
			lastRetryErr = err
		}

		if err := waitChannelMetaLeaderChangeRetry(retryCtx, channelMetaLeaderChangeRetryBackoff); err != nil {
			if callerErr := ctx.Err(); callerErr != nil {
				return ch.Meta{}, callerErr
			}
			return ch.Meta{}, lastRetryErr
		}
	}
}

func retryableChannelMetaLeaderChange(err error) bool {
	return errors.Is(err, errMetaCreateReroute) ||
		errors.Is(err, errMetaCreateRetryMissing) ||
		errors.Is(err, metadb.ErrStaleMeta) ||
		ch.ErrorMatches(err, ch.ErrStaleMeta) ||
		ch.ErrorMatches(err, ch.ErrNotLeader) ||
		errors.Is(err, propose.ErrNotLeader) ||
		errors.Is(err, routing.ErrRouteNotReady) ||
		errors.Is(err, routing.ErrNoSlotLeader) ||
		errors.Is(err, clusternet.ErrNodeNotFound) ||
		errors.Is(err, clusternet.ErrServiceNotFound) ||
		errors.Is(err, transport.ErrStopped) ||
		errors.Is(err, transport.ErrTimeout) ||
		errors.Is(err, transport.ErrNodeNotFound) ||
		errors.Is(err, transport.ErrQueueFull) ||
		errors.Is(err, transport.ErrDialFailed) ||
		errors.Is(err, transport.ErrBusy) ||
		errors.Is(err, context.DeadlineExceeded)
}

func waitChannelMetaLeaderChangeRetry(ctx context.Context, backoff time.Duration) error {
	if backoff <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *SlotMetaSource) buildRuntimeMetaBatch(ctx context.Context, plans []runtimeMetaCreatePlanItem) ([]RuntimeMetaCreateItem, error) {
	ids := make([]ch.ChannelID, len(plans))
	routes := make([]routing.Route, len(plans))
	for i, plan := range plans {
		ids[i], routes[i] = plan.id, plan.route
	}
	placements := make([]ChannelPlacement, len(plans))
	if s.opts.Placement != nil {
		var err error
		placements, err = s.opts.Placement.ResolveChannelPlacementBatch(ctx, ids, routes)
		if err != nil {
			return nil, err
		}
		if len(placements) != len(plans) {
			return nil, fmt.Errorf("%w: aligned channel placement batch", ch.ErrInvalidConfig)
		}
	} else {
		for i := range placements {
			placements[i] = ChannelPlacement{
				Leader:   firstNodeID(s.opts.DefaultReplicas),
				Replicas: append([]ch.NodeID(nil), s.opts.DefaultReplicas...),
				ISR:      append([]ch.NodeID(nil), s.opts.DefaultReplicas...),
				MinISR:   s.opts.DefaultMinISR,
			}
		}
	}
	items := make([]RuntimeMetaCreateItem, len(plans))
	for i, plan := range plans {
		meta, err := RuntimeMetaFromPlacement(plan.id, placements[i])
		if err != nil {
			return nil, err
		}
		items[i] = RuntimeMetaCreateItem{HashSlot: plan.route.HashSlot, Meta: meta}
	}
	return items, nil
}

func (s *SlotMetaSource) readRuntimeMeta(ctx context.Context, id ch.ChannelID) (metadb.ChannelRuntimeMeta, error) {
	if s == nil || s.reader == nil {
		return metadb.ChannelRuntimeMeta{}, fmt.Errorf("%w: slot metadata reader is nil", ch.ErrInvalidConfig)
	}
	meta, err := s.reader.GetChannelRuntimeMeta(ctx, id.ID, int64(id.Type))
	if err != nil {
		return metadb.ChannelRuntimeMeta{}, err
	}
	if meta.ChannelID != id.ID || meta.ChannelType != int64(id.Type) {
		return metadb.ChannelRuntimeMeta{}, fmt.Errorf("%w: resolved %s/%d for %v", ch.ErrStaleMeta, meta.ChannelID, meta.ChannelType, id)
	}
	return meta, nil
}

// RuntimeMetaFromPlacement builds one normalized create-only candidate from an
// authoritative placement decision. It is shared by ordinary append creation
// and person-directory prepare batches so both paths use identical metadata.
func RuntimeMetaFromPlacement(id ch.ChannelID, placement ChannelPlacement) (metadb.ChannelRuntimeMeta, error) {
	replicas := projectUint64NodeIDs(placement.Replicas)
	if len(replicas) == 0 {
		return metadb.ChannelRuntimeMeta{}, fmt.Errorf("%w: empty initial channel replicas", ch.ErrInvalidConfig)
	}
	isr := projectUint64NodeIDs(placement.ISR)
	if len(isr) == 0 {
		return metadb.ChannelRuntimeMeta{}, fmt.Errorf("%w: empty initial channel ISR", ch.ErrInvalidConfig)
	}
	replicaSet := make(map[uint64]struct{}, len(replicas))
	for _, replica := range replicas {
		replicaSet[replica] = struct{}{}
	}
	for _, member := range isr {
		if _, ok := replicaSet[member]; !ok {
			return metadb.ChannelRuntimeMeta{}, fmt.Errorf("%w: initial ISR member %d is not a replica", ch.ErrInvalidConfig, member)
		}
	}
	leader := uint64(placement.Leader)
	if leader == 0 {
		leader = isr[0]
	}
	if !uint64NodeIn(isr, leader) {
		return metadb.ChannelRuntimeMeta{}, fmt.Errorf("%w: initial leader %d is not in ISR", ch.ErrInvalidConfig, leader)
	}
	minISR := placement.MinISR
	if minISR <= 0 {
		minISR = 1
	}
	if minISR > len(isr) {
		return metadb.ChannelRuntimeMeta{}, fmt.Errorf("%w: initial min ISR exceeds ISR", ch.ErrInvalidConfig)
	}
	return metadb.NormalizeChannelRuntimeMeta(metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 1,
		LeaderEpoch:  1,
		Leader:       leader,
		Replicas:     replicas,
		ISR:          isr,
		MinISR:       int64(minISR),
		Status:       uint8(ch.StatusActive),
	}), nil
}

func (s *SlotMetaSource) observeMetaStage(stage string, result string, d time.Duration) {
	if s == nil || s.opts.Observer == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	s.opts.Observer.ObserveChannelAppendStage(stage, result, d)
}

func metaStageResult(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, metadb.ErrNotFound) {
		return "miss"
	}
	return "err"
}

func firstNodeID(nodes []ch.NodeID) ch.NodeID {
	if len(nodes) == 0 {
		return 0
	}
	return nodes[0]
}
