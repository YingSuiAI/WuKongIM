package message

import (
	"context"
	"strings"

	channelmembers "github.com/WuKongIM/WuKongIM/internal/contracts/channelmembers"
)

// CheckChannelDenylist reads the same Channel-owned denylist facts as SEND.
// A service caller supplies a bounded group UID batch; one authority error
// makes the entire check unavailable rather than returning partial results.
func (a *App) CheckChannelDenylist(ctx context.Context, channelID string, channelType uint8, uids []string) ([]string, []string, error) {
	if ctx == nil || channelID == "" || strings.TrimSpace(channelID) != channelID || channelType != channelTypeGroup || len(uids) == 0 || len(uids) > 200 {
		return nil, nil, ErrInvalidCommand
	}
	if a == nil || a.permissionBatch == nil {
		return nil, nil, ErrRouteNotReady
	}
	seen := make(map[string]struct{}, len(uids))
	for _, uid := range uids {
		if uid == "" || strings.TrimSpace(uid) != uid {
			return nil, nil, ErrInvalidCommand
		}
		if _, duplicate := seen[uid]; duplicate {
			return nil, nil, ErrInvalidCommand
		}
		seen[uid] = struct{}{}
	}
	denyID := channelmembers.DenylistChannelID(channelmembers.ChannelKey{ChannelID: channelID, ChannelType: channelType})
	denied := make([]string, 0, len(uids))
	allowed := make([]string, 0, len(uids))
	reads := make([]PermissionRead, len(uids))
	for i, uid := range uids {
		reads[i] = PermissionRead{Kind: PermissionReadSubscriberContains, ChannelID: denyID, ChannelType: int64(channelType), UID: uid}
	}
	facts := a.permissionBatch.ReadPermissionsBatch(ctx, reads)
	if len(facts) != len(reads) {
		return nil, nil, ErrRouteNotReady
	}
	for i, fact := range facts {
		if fact.Err != nil {
			return nil, nil, fact.Err
		}
		if fact.Value {
			denied = append(denied, uids[i])
		} else {
			allowed = append(allowed, uids[i])
		}
	}
	return denied, allowed, nil
}
