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
	for _, uid := range uids {
		membership, ok, err := a.memberships.GetUserChannelMembership(ctx, uid, channelID, int64(channelType))
		if err != nil {
			return nil, nil, err
		}
		if !ok || membership.Tombstone {
			missing = append(missing, uid)
			continue
		}
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
		if fact.ChannelFound && fact.Subscriber && !fact.Disband {
			ready = append(ready, candidate.UID)
			continue
		}
		if fact.SubscriberMutationVersion > candidate.SourceVersion {
			_ = a.membershipAuthority.TombstoneRevokedMembership(ctx, candidate, fact.SubscriberMutationVersion, a.now().UnixNano())
		}
		missing = append(missing, candidate.UID)
	}
	return ready, missing, nil
}
