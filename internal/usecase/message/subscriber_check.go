package message

import (
	"context"

	channelmembers "github.com/WuKongIM/WuKongIM/internal/contracts/channelmembers"
)

// CheckChannelSubscribers confirms the two membership facts used by history
// admission. The caller supplies a bounded service-only batch of group UIDs.
func (a *App) CheckChannelSubscribers(ctx context.Context, channelID string, channelType uint8, uids []string) ([]string, []string, error) {
	if a == nil || a.memberships == nil || a.membershipAuthority == nil {
		return nil, nil, ErrSyncMembershipRequired
	}
	ready := make([]string, 0, len(uids))
	missing := make([]string, 0, len(uids))
	candidates := make([]channelmembers.LiveMembership, 0, len(uids))
	uidRows := make([]struct {
		found     bool
		tombstone bool
	}, 0, len(uids))
	for _, uid := range uids {
		membership, ok, err := a.memberships.GetUserChannelMembership(ctx, uid, channelID, int64(channelType))
		if err != nil {
			return nil, nil, err
		}
		uidRows = append(uidRows, struct {
			found     bool
			tombstone bool
		}{found: ok, tombstone: membership.Tombstone})
		candidates = append(candidates, channelmembers.LiveMembership{
			UID: uid, ChannelID: channelID, ChannelType: int64(channelType), SourceVersion: membership.SourceVersion,
		})
	}
	facts := a.membershipAuthority.AuthorizeLiveMemberships(ctx, candidates)
	if len(facts) != len(candidates) {
		return nil, nil, ErrSyncMembershipRequired
	}
	for index, fact := range facts {
		candidate := candidates[index]
		if fact.Err != nil {
			return nil, nil, fact.Err
		}
		channelLive := fact.ChannelFound && fact.Subscriber && !fact.Disband
		uidLive := uidRows[index].found && !uidRows[index].tombstone
		if channelLive && uidLive {
			ready = append(ready, candidate.UID)
			continue
		}
		if channelLive || uidLive || fact.Subscriber {
			// A one-sided result cannot prove removal. Reconciler must retry
			// its UID projection and read both facts again before advancing.
			return nil, nil, ErrSubscriberMembershipSplit
		}
		missing = append(missing, candidate.UID)
	}
	return ready, missing, nil
}
