package cluster

import (
	"context"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
)

// RejoinUserChannelMembership applies a guarded Platform epoch on the UID Slot.
func (n *Node) RejoinUserChannelMembership(ctx context.Context, rejoin metadb.PlatformMembershipRejoin) error {
	command, err := metafsm.EncodeRejoinUserChannelMembershipCommandChecked(rejoin)
	if err != nil {
		return err
	}
	result, err := n.ProposeResult(ctx, ProposeRequest{Key: rejoin.UID, Command: command})
	if err != nil {
		return err
	}
	if string(result) != metafsm.ApplyResultOK {
		return metadb.ErrStaleMeta
	}
	n.observeMembershipMutation("ordinary", "service_rejoin", 1)
	return nil
}
