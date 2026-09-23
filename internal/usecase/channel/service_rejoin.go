package channel

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

var (
	// ErrServiceRejoinInvalid reports an invalid trusted rejoin boundary.
	ErrServiceRejoinInvalid = errors.New("internal/usecase/channel: invalid service rejoin")
	// ErrServiceRejoinConflict reports an epoch or membership-state mismatch.
	ErrServiceRejoinConflict = errors.New("internal/usecase/channel: service rejoin conflict")
)

// ServiceRejoinCommand carries Platform's durable membership-epoch boundary.
// Only a service-authenticated entry point may construct this command.
type ServiceRejoinCommand struct {
	ChannelID                 string
	ChannelType               uint8
	UID                       string
	MembershipEpoch           uint64
	PreviousRemovedMessageSeq uint64
	JoinedMessageSeq          uint64
	RepairSameEpoch           bool
	// ExpectedDeletedToSeq is set only by the strict read-back endpoint.
	ExpectedDeletedToSeq *uint64
}

// ServiceRejoinState reports the authoritative UID and Channel membership facts.
type ServiceRejoinState struct {
	Ready           bool
	MembershipEpoch uint64
	JoinSeq         uint64
	DeletedToSeq    uint64
	SourceVersion   uint64
}

type serviceRejoinMembershipIndex interface {
	GetUserChannelMembership(context.Context, string, string, int64) (metadb.UserChannelMembership, bool, error)
	RejoinUserChannelMembership(context.Context, metadb.PlatformMembershipRejoin) error
}

func (a *App) serviceRejoinDependencies() (countedSubscriberStore, subscriberLookupStore, serviceRejoinMembershipIndex, error) {
	if a == nil || a.store == nil || a.membershipIndex == nil || a.committedTail == nil {
		return nil, nil, nil, ErrStoreRequired
	}
	counted, ok := a.store.(countedSubscriberStore)
	if !ok {
		return nil, nil, nil, ErrStoreRequired
	}
	lookup, ok := a.store.(subscriberLookupStore)
	if !ok {
		return nil, nil, nil, ErrStoreRequired
	}
	index, ok := a.membershipIndex.(serviceRejoinMembershipIndex)
	if !ok {
		return nil, nil, nil, ErrStoreRequired
	}
	return counted, lookup, index, nil
}

func validateServiceRejoin(cmd ServiceRejoinCommand) error {
	if cmd.ChannelID == "" || strings.TrimSpace(cmd.ChannelID) != cmd.ChannelID ||
		cmd.UID == "" || strings.TrimSpace(cmd.UID) != cmd.UID ||
		cmd.ChannelType != 2 || cmd.MembershipEpoch == 0 ||
		cmd.JoinedMessageSeq == math.MaxUint64 ||
		cmd.PreviousRemovedMessageSeq > cmd.JoinedMessageSeq ||
		(cmd.ExpectedDeletedToSeq != nil && *cmd.ExpectedDeletedToSeq < cmd.JoinedMessageSeq) {
		return ErrServiceRejoinInvalid
	}
	return nil
}

// RejoinSubscriber restores one tombstoned UID with Platform's persisted
// boundary. A repeated exact epoch is idempotent only when both authoritative
// membership facts remain live.
func (a *App) RejoinSubscriber(ctx context.Context, cmd ServiceRejoinCommand) (ServiceRejoinState, error) {
	if err := validateServiceRejoin(cmd); err != nil {
		return ServiceRejoinState{}, err
	}
	counted, _, index, err := a.serviceRejoinDependencies()
	if err != nil {
		return ServiceRejoinState{}, err
	}
	channel, err := a.store.GetChannel(ctx, cmd.ChannelID, int64(cmd.ChannelType))
	if err != nil {
		return ServiceRejoinState{}, err
	}
	if channel.Disband != 0 {
		return ServiceRejoinState{}, ErrServiceRejoinConflict
	}
	committedHead, err := a.committedTail.CommittedChannelTail(ctx, cmd.ChannelID, int64(cmd.ChannelType))
	if err != nil {
		return ServiceRejoinState{}, err
	}
	if cmd.JoinedMessageSeq > committedHead {
		return ServiceRejoinState{}, ErrServiceRejoinConflict
	}
	row, found, err := index.GetUserChannelMembership(ctx, cmd.UID, cmd.ChannelID, int64(cmd.ChannelType))
	if err != nil {
		return ServiceRejoinState{}, err
	}
	if !found {
		return ServiceRejoinState{}, ErrServiceRejoinConflict
	}
	if !row.Tombstone && row.PlatformMembershipEpoch == cmd.MembershipEpoch {
		state, err := a.CheckRejoinSubscriber(ctx, cmd)
		if err != nil {
			return ServiceRejoinState{}, err
		}
		if state.Ready {
			// A prior attempt may have committed both Slot facts but failed
			// before refreshing the derived large-group flag.
			if err := a.refreshRejoinDerivedState(ctx, cmd); err != nil {
				return ServiceRejoinState{}, err
			}
			return state, nil
		}
		return ServiceRejoinState{}, ErrServiceRejoinConflict
	}
	if row.PlatformMembershipEpoch > cmd.MembershipEpoch ||
		(row.PlatformMembershipEpoch == cmd.MembershipEpoch && (!cmd.RepairSameEpoch || row.JoinSeq != cmd.JoinedMessageSeq+1)) {
		return ServiceRejoinState{}, ErrServiceRejoinConflict
	}
	if channel.SubscriberMutationVersion == math.MaxUint64 {
		return ServiceRejoinState{}, ErrServiceRejoinConflict
	}
	// A previous attempt may have committed Channel ownership before its UID
	// projection. Repeating the bounded add is safe and assigns a new fence.
	result, err := counted.AddChannelSubscribersCounted(ctx, cmd.ChannelID, int64(cmd.ChannelType), []string{cmd.UID}, channel.SubscriberMutationVersion+1)
	if err != nil {
		return ServiceRejoinState{}, err
	}
	if result.Version == 0 {
		return ServiceRejoinState{}, metadb.ErrStaleMeta
	}
	rejoin := metadb.PlatformMembershipRejoin{
		UID: cmd.UID, ChannelID: cmd.ChannelID, ChannelType: int64(cmd.ChannelType),
		MembershipEpoch: cmd.MembershipEpoch, PreviousRemovedSeq: cmd.PreviousRemovedMessageSeq,
		JoinedSeq: cmd.JoinedMessageSeq, RepairSameEpoch: cmd.RepairSameEpoch,
		SourceVersion: result.Version, UpdatedAt: a.now().UnixNano(),
	}
	if err := index.RejoinUserChannelMembership(ctx, rejoin); err != nil {
		// A remote UID Slot may have committed while its response was lost.
		// A strict read-back avoids removing a fully restored member.
		if state, checkErr := a.CheckRejoinSubscriber(ctx, cmd); checkErr == nil && state.Ready {
			if refreshErr := a.refreshRejoinDerivedState(ctx, cmd); refreshErr != nil {
				return ServiceRejoinState{}, refreshErr
			}
			return state, nil
		}
		return ServiceRejoinState{}, a.compensateFailedRejoin(ctx, cmd, err)
	}
	state, err := a.CheckRejoinSubscriber(ctx, cmd)
	if err != nil {
		return ServiceRejoinState{}, err
	}
	if !state.Ready {
		return state, ErrServiceRejoinConflict
	}
	if err := a.refreshRejoinDerivedState(ctx, cmd); err != nil {
		return ServiceRejoinState{}, err
	}
	return state, nil
}

func (a *App) compensateFailedRejoin(ctx context.Context, cmd ServiceRejoinCommand, cause error) error {
	// Channel and UID live in different Slots. If UID did not acknowledge the
	// epoch write, remove the Channel member promptly to narrow the realtime
	// versus history split. Platform keeps its durable barrier pending and
	// retries. This is best effort: a failed compensation remains visible.
	compensationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := a.RemoveSubscribers(compensationCtx, SubscriberCommand{ChannelID: cmd.ChannelID, ChannelType: cmd.ChannelType, Subscribers: []string{cmd.UID}}); err != nil {
		return errors.Join(cause, fmt.Errorf("compensating subscriber removal: %w", err))
	}
	return cause
}

func (a *App) refreshRejoinDerivedState(ctx context.Context, cmd ServiceRejoinCommand) error {
	channel, err := a.refreshLargeGroupFlag(ctx, cmd.ChannelID, int64(cmd.ChannelType))
	if err != nil {
		return err
	}
	a.notifySubscriberMutation(ctx, channel, false, []string{cmd.UID}, nil)
	return nil
}

// CheckRejoinSubscriber reads both Slot-owned facts and requires the exact
// Platform epoch and history floor before reporting readiness.
func (a *App) CheckRejoinSubscriber(ctx context.Context, cmd ServiceRejoinCommand) (ServiceRejoinState, error) {
	if err := validateServiceRejoin(cmd); err != nil {
		return ServiceRejoinState{}, err
	}
	_, lookup, index, err := a.serviceRejoinDependencies()
	if err != nil {
		return ServiceRejoinState{}, err
	}
	channel, err := a.store.GetChannel(ctx, cmd.ChannelID, int64(cmd.ChannelType))
	if err != nil {
		return ServiceRejoinState{}, err
	}
	row, found, err := index.GetUserChannelMembership(ctx, cmd.UID, cmd.ChannelID, int64(cmd.ChannelType))
	if err != nil {
		return ServiceRejoinState{}, err
	}
	if !found {
		return ServiceRejoinState{}, nil
	}
	state := ServiceRejoinState{MembershipEpoch: row.PlatformMembershipEpoch, JoinSeq: row.JoinSeq, DeletedToSeq: row.DeletedToSeq, SourceVersion: row.SourceVersion}
	member, err := lookup.ContainsChannelSubscriber(ctx, cmd.ChannelID, int64(cmd.ChannelType), cmd.UID)
	if err != nil {
		return ServiceRejoinState{}, err
	}
	state.Ready = member && channel.Disband == 0 && !row.Tombstone && row.PlatformMembershipEpoch == cmd.MembershipEpoch &&
		row.JoinSeq == cmd.JoinedMessageSeq+1 && row.DeletedToSeq >= cmd.JoinedMessageSeq &&
		row.SourceVersion != 0 && row.SourceVersion <= channel.SubscriberMutationVersion
	if cmd.ExpectedDeletedToSeq != nil {
		state.Ready = state.Ready && row.DeletedToSeq == *cmd.ExpectedDeletedToSeq
	}
	return state, nil
}
