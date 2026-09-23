package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

const maxSubscriberCheckUIDs = 200

type channelSubscriberCheckRequest struct {
	ChannelID   string   `json:"channel_id"`
	ChannelType uint8    `json:"channel_type"`
	Subscribers []string `json:"subscribers"`
}

type channelSubscriberChecker interface {
	CheckChannelSubscribers(context.Context, string, uint8, []string) ([]string, []string, error)
}

func (s *Server) handleChannelSubscriberCheck(c *gin.Context) {
	var req channelSubscriberCheckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid subscriber check request"})
		return
	}
	if req.ChannelID == "" || strings.TrimSpace(req.ChannelID) != req.ChannelID || req.ChannelType != 2 || len(req.Subscribers) == 0 || len(req.Subscribers) > maxSubscriberCheckUIDs {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid subscriber check request"})
		return
	}
	seen := make(map[string]struct{}, len(req.Subscribers))
	for _, uid := range req.Subscribers {
		if uid == "" || strings.TrimSpace(uid) != uid {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid subscriber check request"})
			return
		}
		if _, ok := seen[uid]; ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid subscriber check request"})
			return
		}
		seen[uid] = struct{}{}
	}
	checker, ok := s.messages.(channelSubscriberChecker)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "subscriber check unavailable"})
		return
	}
	ready, missing, err := checker.CheckChannelSubscribers(c.Request.Context(), req.ChannelID, req.ChannelType, req.Subscribers)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "subscriber check unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ready": ready, "missing": missing})
}
