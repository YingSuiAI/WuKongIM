package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type channelBlacklistCheckRequest struct {
	ChannelID   string   `json:"channel_id"`
	ChannelType uint8    `json:"channel_type"`
	UIDs        []string `json:"uids"`
}

type channelBlacklistChecker interface {
	CheckChannelDenylist(context.Context, string, uint8, []string) ([]string, []string, error)
}

// handleChannelBlacklistCheck returns exact Slot-owned denylist facts for one
// bounded group batch. It never exposes a partial success on authority errors.
func (s *Server) handleChannelBlacklistCheck(c *gin.Context) {
	var req channelBlacklistCheckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid blacklist check request"})
		return
	}
	if req.ChannelID == "" || strings.TrimSpace(req.ChannelID) != req.ChannelID || req.ChannelType != 2 || len(req.UIDs) == 0 || len(req.UIDs) > maxSubscriberCheckUIDs {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid blacklist check request"})
		return
	}
	seen := make(map[string]struct{}, len(req.UIDs))
	for _, uid := range req.UIDs {
		if uid == "" || strings.TrimSpace(uid) != uid {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid blacklist check request"})
			return
		}
		if _, duplicate := seen[uid]; duplicate {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid blacklist check request"})
			return
		}
		seen[uid] = struct{}{}
	}
	checker, ok := s.messages.(channelBlacklistChecker)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "blacklist check unavailable"})
		return
	}
	denied, allowed, err := checker.CheckChannelDenylist(c.Request.Context(), req.ChannelID, req.ChannelType, req.UIDs)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "blacklist check unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"denied": denied, "allowed": allowed})
}
