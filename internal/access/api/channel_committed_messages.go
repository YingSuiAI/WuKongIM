package api

import (
	"errors"
	"net/http"
	"strings"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	"github.com/gin-gonic/gin"
)

type committedMessagesRequest struct {
	ChannelID       string `json:"channel_id"`
	ChannelType     uint8  `json:"channel_type"`
	AfterMessageSeq uint64 `json:"after_message_seq"`
	Limit           int    `json:"limit"`
	ScanHead        uint64 `json:"scan_head"`
}

type committedMessagesResponse struct {
	ScanHead                 uint64              `json:"scan_head"`
	FirstAvailableMessageSeq uint64              `json:"first_available_message_seq"`
	RetentionGap             bool                `json:"retention_gap"`
	NextAfterMessageSeq      uint64              `json:"next_after_message_seq"`
	HasMore                  bool                `json:"has_more"`
	Messages                 []legacyMessageResp `json:"messages"`
}

func (s *Server) handleChannelCommittedMessages(c *gin.Context) {
	var req committedMessagesRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.ChannelID == "" || strings.TrimSpace(req.ChannelID) != req.ChannelID ||
		req.ChannelType == 0 || req.Limit < 1 || req.Limit > channelusecase.MaxCommittedMessagesPageLimit ||
		(req.ScanHead > 0 && req.AfterMessageSeq > req.ScanHead) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid committed messages query"})
		return
	}
	reader, ok := s.channels.(channelusecase.CommittedMessagesReader)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "committed messages unavailable"})
		return
	}
	page, found, err := reader.ReadCommittedMessages(c.Request.Context(), channelusecase.ChannelKey{
		ChannelID: req.ChannelID, ChannelType: req.ChannelType,
	}, channelusecase.CommittedMessagesQuery{
		AfterMessageSeq: req.AfterMessageSeq, Limit: req.Limit, ScanHead: req.ScanHead,
	})
	if errors.Is(err, channelusecase.ErrCommittedMessagesQuery) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid committed messages query"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "committed messages unavailable"})
		return
	}
	if !found {
		if req.AfterMessageSeq != 0 || req.ScanHead != 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
			return
		}
		// Committed head defines an unknown, never-created Channel as head zero.
		// Its initial recovery page is therefore the empty terminal page for the
		// same frozen head, while non-zero cursors remain absent.
		page = channelusecase.CommittedMessagesPage{}
	}
	response := committedMessagesResponse{
		ScanHead: page.ScanHead, FirstAvailableMessageSeq: page.FirstAvailableMessageSeq,
		RetentionGap: page.RetentionGap, NextAfterMessageSeq: page.NextAfterMessageSeq,
		HasMore: page.HasMore, Messages: make([]legacyMessageResp, len(page.Messages)),
	}
	for index, message := range page.Messages {
		response.Messages[index] = committedMessageLegacyResponse(message)
	}
	c.JSON(http.StatusOK, response)
}
