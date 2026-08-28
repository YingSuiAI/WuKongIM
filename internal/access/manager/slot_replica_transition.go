package manager

import (
	"context"
	"errors"
	"net/http"

	managementusecase "github.com/WuKongIM/WuKongIM/internal/usecase/management"
	"github.com/gin-gonic/gin"
)

type slotReplicaTransitionManagement interface {
	AdvanceSlotReplicaTransition(context.Context, managementusecase.SlotReplicaTransitionRequest) (managementusecase.SlotReplicaTransitionResponse, error)
}

// ManagerSlotReplicaTransitionRequest selects the exact final three-node topology.
type ManagerSlotReplicaTransitionRequest struct {
	TargetNodeIDs []uint64 `json:"target_node_ids"`
}

// ManagerSlotReplicaTransitionResponse exposes bounded, resumable transition progress.
type ManagerSlotReplicaTransitionResponse struct {
	GeneratedAt        string       `json:"generated_at"`
	StateRevision      uint64       `json:"state_revision"`
	SourceReplicaCount uint16       `json:"source_replica_count"`
	TargetReplicaCount uint16       `json:"target_replica_count"`
	CompletedSlots     int          `json:"completed_slots"`
	TotalSlots         int          `json:"total_slots"`
	Phase              string       `json:"phase"`
	Created            bool         `json:"created"`
	Task               *SlotTaskDTO `json:"task,omitempty"`
}

func (s *Server) handleSlotReplicaTransitionAdvance(c *gin.Context) {
	management, ok := s.management.(slotReplicaTransitionManagement)
	if !ok {
		jsonError(c, http.StatusServiceUnavailable, "service_unavailable", "slot replica transition is unavailable")
		return
	}
	var body ManagerSlotReplicaTransitionRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		jsonError(c, http.StatusBadRequest, "bad_request", "bad_request")
		return
	}
	response, err := management.AdvanceSlotReplicaTransition(c.Request.Context(), managementusecase.SlotReplicaTransitionRequest{TargetNodeIDs: body.TargetNodeIDs})
	if err != nil {
		if errors.Is(err, managementusecase.ErrSlotReplicaTransitionInvalid) {
			jsonError(c, http.StatusConflict, "slot_replica_transition_invalid", err.Error())
			return
		}
		jsonError(c, http.StatusServiceUnavailable, "slot_replica_transition_unavailable", err.Error())
		return
	}
	status := http.StatusOK
	if response.Created {
		status = http.StatusAccepted
	}
	c.JSON(status, ManagerSlotReplicaTransitionResponse{
		GeneratedAt: managerTimeString(response.GeneratedAt), StateRevision: response.StateRevision,
		SourceReplicaCount: response.SourceReplicaCount, TargetReplicaCount: response.TargetReplicaCount,
		CompletedSlots: response.CompletedSlots, TotalSlots: response.TotalSlots,
		Phase: response.Phase, Created: response.Created, Task: slotTaskDTO(response.Task),
	})
}
