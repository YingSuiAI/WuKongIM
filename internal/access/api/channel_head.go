package api

import (
	"net/http"
	"strings"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	"github.com/gin-gonic/gin"
)

func (s *Server) handleChannelCommittedHead(c *gin.Context) {
	var req channelKeyRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.ChannelID == "" || strings.TrimSpace(req.ChannelID) != req.ChannelID || req.ChannelType == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid channel"})
		return
	}
	reader, ok := s.channels.(channelusecase.CommittedHeadReader)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "committed head unavailable"})
		return
	}
	seq, err := reader.ReadCommittedHead(c.Request.Context(), channelusecase.ChannelKey{ChannelID: req.ChannelID, ChannelType: req.ChannelType})
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "committed head unavailable"})
		return
	}
	c.JSON(http.StatusOK, struct {
		ChannelID           string `json:"channel_id"`
		ChannelType         uint8  `json:"channel_type"`
		CommittedMessageSeq uint64 `json:"committed_message_seq"`
	}{req.ChannelID, req.ChannelType, seq})
}
