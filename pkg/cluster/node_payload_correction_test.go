package cluster

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/messagepayload"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/stretchr/testify/require"
)

func TestPayloadCorrectionNewLeaderCannotReadCommittedButUnappliedAbsence(t *testing.T) {
	status := multiraft.Status{Role: multiraft.RoleLeader, LeaderID: 1, Term: 3, CommitIndex: 20, AppliedIndex: 19}
	require.ErrorIs(t, payloadCorrectionReadAuthority(status, 1), messagepayload.ErrUnavailable)
	status.AppliedIndex = 20
	require.NoError(t, payloadCorrectionReadAuthority(status, 1))
	status.Role = multiraft.RoleFollower
	require.ErrorIs(t, payloadCorrectionReadAuthority(status, 1), ErrNotLeader)
}
