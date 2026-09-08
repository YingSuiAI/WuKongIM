package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"

	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	"github.com/gin-gonic/gin"
)

type messagePayloadCorrectionUsecase interface {
	CorrectMessagePayload(context.Context, channelusecase.MessagePayloadCorrection) (channelusecase.PayloadCorrectionReceipt, error)
	LookupCommittedMessageClaim(context.Context, channelusecase.CommittedMessageClaim) (channelusecase.CommittedMessage, bool, error)
}

type payloadCorrectionRequest struct {
	OperationID                   string `json:"operation_id"`
	ChannelID                     string `json:"channel_id"`
	ChannelType                   uint8  `json:"channel_type"`
	MessageID                     uint64 `json:"message_id"`
	MessageSeq                    uint64 `json:"message_seq"`
	FromUID                       string `json:"from_uid"`
	ClientMsgNo                   string `json:"client_msg_no"`
	ExpectedOriginalPayloadSHA256 string `json:"expected_original_payload_sha256"`
	CorrectedPayloadSHA256        string `json:"corrected_payload_sha256"`
	CorrectedPayload              []byte `json:"corrected_payload"`
}

type committedMessageClaimRequest struct {
	ChannelID                     string `json:"channel_id"`
	ChannelType                   uint8  `json:"channel_type"`
	FromUID                       string `json:"from_uid"`
	ClientMsgNo                   string `json:"client_msg_no"`
	ExpectedOriginalPayloadSHA256 string `json:"expected_original_payload_sha256"`
}

func (s *Server) handleMessagePayloadCorrection(c *gin.Context) {
	var request payloadCorrectionRequest
	if !decodePayloadMaintenanceRequest(c, &request) || request.MessageID > math.MaxInt64 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload correction request"})
		return
	}
	usecase, ok := s.channels.(messagePayloadCorrectionUsecase)
	if !ok {
		writePayloadCorrectionError(c, nil)
		return
	}
	receipt, err := usecase.CorrectMessagePayload(c.Request.Context(), channelusecase.MessagePayloadCorrection{
		OperationID: request.OperationID, ChannelID: request.ChannelID, ChannelType: int64(request.ChannelType),
		MessageID: request.MessageID, MessageSeq: request.MessageSeq, FromUID: request.FromUID, ClientMsgNo: request.ClientMsgNo,
		OriginalPayloadSHA256: request.ExpectedOriginalPayloadSHA256, CorrectedPayloadSHA256: request.CorrectedPayloadSHA256,
		CorrectedPayload: request.CorrectedPayload,
	})
	if err != nil {
		writePayloadCorrectionError(c, err)
		return
	}
	c.JSON(http.StatusOK, receipt)
}

func (s *Server) handleCommittedMessageClaim(c *gin.Context) {
	var request committedMessageClaimRequest
	if !decodePayloadMaintenanceRequest(c, &request) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid committed claim request"})
		return
	}
	usecase, ok := s.channels.(messagePayloadCorrectionUsecase)
	if !ok {
		writePayloadCorrectionError(c, nil)
		return
	}
	message, found, err := usecase.LookupCommittedMessageClaim(c.Request.Context(), channelusecase.CommittedMessageClaim{
		ChannelID: request.ChannelID, ChannelType: request.ChannelType, FromUID: request.FromUID, ClientMsgNo: request.ClientMsgNo,
		ExpectedOriginalPayloadSHA256: request.ExpectedOriginalPayloadSHA256,
	})
	if err != nil {
		writePayloadCorrectionError(c, err)
		return
	}
	if !found {
		c.JSON(http.StatusOK, gin.H{"state": "not_committed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"state": "committed", "message": committedMessageLegacyResponse(message)})
}

func decodePayloadMaintenanceRequest(c *gin.Context, target any) bool {
	// A 256 KiB byte payload expands under base64. This bound includes that
	// encoding and fixed identity fields, without an unbounded maintenance body.
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 512*1024))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return false
	}
	return decoder.Decode(&struct{}{}) == io.EOF
}

func writePayloadCorrectionError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, channelusecase.ErrPayloadCorrectionInvalid):
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload correction identity or digest"})
	case errors.Is(err, channelusecase.ErrPayloadCorrectionConflict):
		c.JSON(http.StatusConflict, gin.H{"error": "payload correction conflicts with immutable identity"})
	case errors.Is(err, channelusecase.ErrPayloadCorrectionNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "committed message not found"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "payload correction authority unavailable"})
	}
}
