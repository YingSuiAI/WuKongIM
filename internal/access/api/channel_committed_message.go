package api

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	"github.com/gin-gonic/gin"
)

type committedMessageRequest struct {
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
	MessageID   uint64 `json:"message_id"`
	MessageSeq  uint64 `json:"message_seq"`
}

func (s *Server) handleChannelCommittedMessage(c *gin.Context) {
	var req committedMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.ChannelID == "" || strings.TrimSpace(req.ChannelID) != req.ChannelID ||
		req.ChannelType == 0 || req.MessageID == 0 || req.MessageID > math.MaxInt64 || req.MessageSeq == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid committed message identity"})
		return
	}
	reader, ok := s.channels.(channelusecase.CommittedMessageReader)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "committed message unavailable"})
		return
	}
	message, found, err := reader.ReadCommittedMessage(c.Request.Context(), channelusecase.ChannelKey{
		ChannelID: req.ChannelID, ChannelType: req.ChannelType,
	}, channelusecase.CommittedMessageIdentity{MessageID: req.MessageID, MessageSeq: req.MessageSeq})
	if errors.Is(err, channelusecase.ErrCommittedMessageIdentity) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid committed message identity"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "committed message unavailable"})
		return
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "committed message not found"})
		return
	}
	c.JSON(http.StatusOK, committedMessageLegacyResponse(message))
}

func committedMessageLegacyResponse(message channelusecase.CommittedMessage) legacyMessageResp {
	return legacyMessageResp{
		Header:  legacyMessageHeader{SyncOnce: boolToInt(message.SyncOnce)},
		Setting: message.Setting, MessageID: int64(message.MessageID), MessageIDStr: strconv.FormatUint(message.MessageID, 10),
		ClientMsgNo: message.ClientMsgNo, MessageSeq: message.MessageSeq,
		FromUID: message.FromUID, ChannelID: message.ChannelID, ChannelType: message.ChannelType,
		Timestamp: int32(message.ServerTimestampMS / 1000), Payload: append([]byte(nil), message.Payload...),
	}
}
