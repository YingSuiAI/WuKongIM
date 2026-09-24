package api

import (
	"context"
	"errors"
	"net/http"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/gin-gonic/gin"
)

type channelServiceRejoinRequest struct {
	ChannelID                      string  `json:"channel_id"`
	ChannelType                    uint8   `json:"channel_type"`
	UID                            string  `json:"uid"`
	MembershipEpoch                *uint64 `json:"membership_epoch"`
	PreviousRemovedMessageSequence *uint64 `json:"previous_removed_message_sequence"`
	JoinedMessageSequence          *uint64 `json:"joined_message_sequence"`
	RepairSameEpoch                bool    `json:"repair_same_epoch"`
}

type channelServiceRejoinCheckRequest struct {
	ChannelID       string  `json:"channel_id"`
	ChannelType     uint8   `json:"channel_type"`
	UID             string  `json:"uid"`
	MembershipEpoch *uint64 `json:"membership_epoch"`
	JoinSeq         *uint64 `json:"join_seq"`
	DeletedToSeq    *uint64 `json:"deleted_to_seq"`
}

func (r channelServiceRejoinRequest) command() channelusecase.ServiceRejoinCommand {
	return channelusecase.ServiceRejoinCommand{
		ChannelID: r.ChannelID, ChannelType: r.ChannelType, UID: r.UID,
		MembershipEpoch: *r.MembershipEpoch, PreviousRemovedMessageSeq: *r.PreviousRemovedMessageSequence,
		JoinedMessageSeq: *r.JoinedMessageSequence, RepairSameEpoch: r.RepairSameEpoch,
	}
}

type serviceRejoinUsecase interface {
	RejoinSubscriber(context.Context, channelusecase.ServiceRejoinCommand) (channelusecase.ServiceRejoinState, error)
	CheckRejoinSubscriber(context.Context, channelusecase.ServiceRejoinCommand) (channelusecase.ServiceRejoinState, error)
}

func (s *Server) handleChannelServiceRejoin(c *gin.Context) {
	var req channelServiceRejoinRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.MembershipEpoch == nil || req.PreviousRemovedMessageSequence == nil || req.JoinedMessageSequence == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid service rejoin request"})
		return
	}
	service, ok := s.channels.(serviceRejoinUsecase)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "service rejoin unavailable"})
		return
	}
	state, err := service.RejoinSubscriber(c.Request.Context(), req.command())
	if err != nil {
		writeServiceRejoinError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"membership_epoch": state.MembershipEpoch,
		"join_seq":         state.JoinSeq,
		"deleted_to_seq":   state.DeletedToSeq,
		"source_version":   state.SourceVersion,
	})
}

func (s *Server) handleChannelServiceRejoinCheck(c *gin.Context) {
	var req channelServiceRejoinCheckRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.MembershipEpoch == nil || req.JoinSeq == nil || req.DeletedToSeq == nil || *req.JoinSeq == 0 || *req.DeletedToSeq < *req.JoinSeq-1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid service rejoin check request"})
		return
	}
	service, ok := s.channels.(serviceRejoinUsecase)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "service rejoin unavailable"})
		return
	}
	state, err := service.CheckRejoinSubscriber(c.Request.Context(), channelusecase.ServiceRejoinCommand{
		ChannelID: req.ChannelID, ChannelType: req.ChannelType, UID: req.UID,
		MembershipEpoch: *req.MembershipEpoch, JoinedMessageSeq: *req.JoinSeq - 1,
		ExpectedDeletedToSeq: req.DeletedToSeq,
	})
	if err != nil {
		writeServiceRejoinError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"ready":                 state.Ready,
		"actual_epoch":          state.MembershipEpoch,
		"actual_join_seq":       state.JoinSeq,
		"actual_deleted_to_seq": state.DeletedToSeq,
		"source_version":        state.SourceVersion,
	})
}

func writeServiceRejoinError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, channelusecase.ErrServiceRejoinInvalid), errors.Is(err, metadb.ErrInvalidArgument):
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid service rejoin request"})
	case errors.Is(err, channelusecase.ErrServiceRejoinConflict), errors.Is(err, metadb.ErrStaleMeta), errors.Is(err, metadb.ErrNotFound):
		c.JSON(http.StatusConflict, gin.H{"error": "service rejoin conflict"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "service rejoin unavailable"})
	}
}
