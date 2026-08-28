package fsm

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/controller/command"
	"github.com/WuKongIM/WuKongIM/pkg/controller/state"
	"github.com/stretchr/testify/require"
)

func TestPromotionReservationAndSlotExpansionAreMutuallyExclusiveAtApply(t *testing.T) {
	for _, reserveFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "expansion_first", true: "reservation_first"}[reserveFirst], func(t *testing.T) {
			sm, _ := initializedStateMachine(t, 1)
			st := sm.Snapshot(context.Background())
			st.Config.SlotCount, st.Config.ReplicaCount = 1, 1
			st.Controllers = st.Controllers[:1]
			st.Nodes[1].Roles = []state.NodeRole{state.NodeRoleData}
			st.Slots = []state.SlotAssignment{{SlotID: 1, ConfigEpoch: 1, DesiredPeers: []uint64{1}, PreferredLeader: 1}}
			var err error
			st.HashSlots, err = state.BuildInitialHashSlotTable(1, st.Config.HashSlotCount)
			require.NoError(t, err)
			require.NoError(t, sm.Restore(context.Background(), st))
			reserve := command.Command{Kind: command.KindReserveControllerVoterPromotion,
				ControllerVoterPromotion: &command.ControllerVoterPromotion{TargetNodeID: 2, TargetAddr: "n2", ExpectedPreviousVoters: []uint64{1}}}
			expand := command.Command{Kind: command.KindUpsertSlotReplicaMoveTask,
				SlotReplicaCountTransition: &state.SlotReplicaCountTransition{SourceReplicaCount: 1, TargetReplicaCount: 3, StartedAtRevision: st.Revision, TargetNodeIDs: []uint64{1, 2, 3}},
				Task: &state.ReconcileTask{TaskID: "add-2", SlotID: 1, Kind: state.TaskKindSlotReplicaMove, Step: state.TaskStepOpenLearner,
					TargetNode: 2, TargetPeers: []uint64{1, 2}, ConfigEpoch: 1, CompletionPolicy: state.TaskCompletionPolicySingleObserver, Status: state.TaskStatusPending}}
			first, second := expand, reserve
			if reserveFirst {
				first, second = reserve, expand
			}
			applyOK(t, sm, 2, first)
			result, err := sm.Apply(context.Background(), 3, second)
			require.NoError(t, err)
			require.True(t, result.Rejected, "second operation must reject even without a stale-revision check")
			if reserveFirst {
				require.Equal(t, ReasonControllerVoterPromotionActive, result.Reason)
				applyOK(t, sm, 4, reserve)
				commit := command.Command{Kind: command.KindPromoteControllerVoter, ControllerVoterPromotion: &command.ControllerVoterPromotion{
					TargetNodeID: 2, TargetAddr: "n2", ExpectedPreviousVoters: []uint64{1}, ObservedConfigIndex: 20, ObservedVoters: []uint64{1, 2}}}
				applyOK(t, sm, 5, commit)
				require.Nil(t, sm.Snapshot(context.Background()).ControllerVoterPromotion)
			} else {
				require.Equal(t, ReasonSlotReplicaCountTransitionActive, result.Reason)
			}
		})
	}
}
