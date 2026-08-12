package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	messageusecase "github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/gin-gonic/gin"
)

type appendMessageEventRequest struct {
	ChannelID          string           `json:"channel_id"`
	ChannelType        uint8            `json:"channel_type"`
	FromUID            string           `json:"from_uid"`
	MessageID          int64            `json:"message_id"`
	ClientMsgNo        string           `json:"client_msg_no"`
	RunID              string           `json:"run_id"`
	AuthorizationFence uint64           `json:"authorization_fence"`
	AuthoritySequence  uint64           `json:"authority_sequence"`
	EventID            string           `json:"event_id"`
	EventType          string           `json:"event_type"`
	EventKey           string           `json:"event_key"`
	Visibility         string           `json:"visibility"`
	OccurredAt         int64            `json:"occurred_at"`
	Payload            json.RawMessage  `json:"payload"`
	Headers            *json.RawMessage `json:"headers"`
}

type appendMessageEventResponse struct {
	ClientMsgNo       string `json:"client_msg_no"`
	RunID             string `json:"run_id"`
	EventKey          string `json:"event_key"`
	EventID           string `json:"event_id"`
	MsgEventSeq       uint64 `json:"msg_event_seq"`
	AuthoritySequence uint64 `json:"authority_sequence"`
	EventStatus       string `json:"event_status"`
	ChannelID         string `json:"channel_id"`
	ChannelType       uint8  `json:"channel_type"`
	FromUID           string `json:"from_uid"`
}

func (s *Server) handleMessageEventAppend(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 320*1024)
	var req appendMessageEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeJSONError(c, "数据格式有误！")
		return
	}
	if s == nil || s.messages == nil {
		writeJSONError(c, "message usecase not configured")
		return
	}
	if req.Headers != nil {
		writeJSONError(c, "message event headers are not supported")
		return
	}
	var messageID uint64
	if req.MessageID > 0 {
		messageID = uint64(req.MessageID)
	}
	result, err := s.messages.AppendMessageEvent(c.Request.Context(), messageusecase.MessageEventAppend{
		ChannelID:          req.ChannelID,
		ChannelType:        int64(req.ChannelType),
		FromUID:            req.FromUID,
		MessageID:          messageID,
		ClientMsgNo:        req.ClientMsgNo,
		RunID:              req.RunID,
		AuthorizationFence: strconv.FormatUint(req.AuthorizationFence, 10),
		AuthoritySequence:  req.AuthoritySequence,
		EventID:            req.EventID,
		EventKey:           req.EventKey,
		EventType:          req.EventType,
		Visibility:         req.Visibility,
		OccurredAt:         req.OccurredAt,
		Payload:            append([]byte(nil), req.Payload...),
	})
	if err != nil {
		writeJSONError(c, err.Error())
		return
	}
	fromUID := result.FromUID
	if fromUID == "" {
		fromUID = req.FromUID
	}
	c.JSON(http.StatusOK, gin.H{
		"status": http.StatusOK,
		"data": appendMessageEventResponse{
			ClientMsgNo:       result.ClientMsgNo,
			RunID:             result.RunID,
			EventKey:          result.EventKey,
			EventID:           result.EventID,
			MsgEventSeq:       result.MsgEventSeq,
			AuthoritySequence: req.AuthoritySequence,
			EventStatus:       result.Status,
			ChannelID:         req.ChannelID,
			ChannelType:       req.ChannelType,
			FromUID:           fromUID,
		},
	})
}
