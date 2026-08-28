package node

import (
	"fmt"

	channelappendcontract "github.com/WuKongIM/WuKongIM/internal/contracts/channelappend"
	"github.com/WuKongIM/WuKongIM/internal/contracts/onlinedelivery"
)

var (
	deliveryRPCRequestMagic  = [...]byte{'W', 'K', 'V', 'D', 2}
	deliveryRPCResponseMagic = [...]byte{'W', 'K', 'V', 'd', 2}
)

const maxDeliveryRPCCollectionLen = 4096

// deliveryPushRequest is the deterministic binary DTO for owner-node delivery push calls.
type deliveryPushRequest struct {
	// Push carries one fence-complete owner-node delivery batch.
	Push onlinedelivery.OwnerPush
}

// deliveryPushResponse is the deterministic binary DTO returned by delivery push calls.
type deliveryPushResponse struct {
	// Status is one of the stable delivery RPC status strings.
	Status string
	// Result reports how the owner node classified pushed routes.
	Result onlinedelivery.OwnerPushResult
}

func encodeDeliveryPushRequest(req deliveryPushRequest) ([]byte, error) {
	dst := make([]byte, 0, 128)
	dst = append(dst, deliveryRPCRequestMagic[:]...)
	dst = appendDeliveryPush(dst, req.Push)
	return dst, nil
}

func decodeDeliveryPushRequest(body []byte) (deliveryPushRequest, error) {
	if !hasMagic(body, deliveryRPCRequestMagic[:]) {
		return deliveryPushRequest{}, fmt.Errorf("internal/access/node: invalid delivery request codec")
	}
	offset := len(deliveryRPCRequestMagic)
	var req deliveryPushRequest
	var err error
	if req.Push, offset, err = readDeliveryPush(body, offset); err != nil {
		return deliveryPushRequest{}, err
	}
	if offset != len(body) {
		return deliveryPushRequest{}, fmt.Errorf("internal/access/node: trailing delivery request bytes")
	}
	return req, nil
}

func encodeDeliveryPushResponse(resp deliveryPushResponse) ([]byte, error) {
	dst := make([]byte, 0, 128)
	dst = append(dst, deliveryRPCResponseMagic[:]...)
	dst = appendString(dst, resp.Status)
	dst = appendDeliveryPushResult(dst, resp.Result)
	return dst, nil
}

func decodeDeliveryPushResponse(body []byte) (deliveryPushResponse, error) {
	if !hasMagic(body, deliveryRPCResponseMagic[:]) {
		return deliveryPushResponse{}, fmt.Errorf("internal/access/node: invalid delivery response codec")
	}
	offset := len(deliveryRPCResponseMagic)
	var resp deliveryPushResponse
	var err error
	if resp.Status, offset, err = readString(body, offset); err != nil {
		return deliveryPushResponse{}, err
	}
	if resp.Result, offset, err = readDeliveryPushResult(body, offset); err != nil {
		return deliveryPushResponse{}, err
	}
	if offset != len(body) {
		return deliveryPushResponse{}, fmt.Errorf("internal/access/node: trailing delivery response bytes")
	}
	return resp, nil
}

func appendDeliveryPush(dst []byte, push onlinedelivery.OwnerPush) []byte {
	dst = appendUvarint(dst, push.OwnerNodeID)
	dst = appendDeliveryEnvelope(dst, push.Event)
	return appendDeliveryEventRoutes(dst, push.Routes)
}

func readDeliveryPush(body []byte, offset int) (onlinedelivery.OwnerPush, int, error) {
	var push onlinedelivery.OwnerPush
	var err error
	if push.OwnerNodeID, offset, err = readUvarint(body, offset); err != nil {
		return onlinedelivery.OwnerPush{}, offset, err
	}
	if push.Event, offset, err = readDeliveryEnvelope(body, offset); err != nil {
		return onlinedelivery.OwnerPush{}, offset, err
	}
	if push.Routes, offset, err = readDeliveryEventRoutes(body, offset); err != nil {
		return onlinedelivery.OwnerPush{}, offset, err
	}
	return push, offset, nil
}

func appendDeliveryEnvelope(dst []byte, env channelappendcontract.CommittedEnvelope) []byte {
	dst = appendUvarint(dst, env.MessageID)
	dst = appendUvarint(dst, env.MessageSeq)
	dst = appendString(dst, env.ChannelID)
	dst = append(dst, env.ChannelType)
	dst = appendString(dst, env.FromUID)
	dst = appendUvarint(dst, env.SenderNodeID)
	dst = appendUvarint(dst, env.SenderSessionID)
	dst = appendString(dst, env.ClientMsgNo)
	if env.RedDot {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = appendBytes(dst, env.Payload)
	return appendStringSlice(dst, env.MessageScopedUIDs)
}

func readDeliveryEnvelope(body []byte, offset int) (channelappendcontract.CommittedEnvelope, int, error) {
	var env channelappendcontract.CommittedEnvelope
	var redDot byte
	var err error
	if env.MessageID, offset, err = readUvarint(body, offset); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	if env.MessageSeq, offset, err = readUvarint(body, offset); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	if env.ChannelID, offset, err = readString(body, offset); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	if env.ChannelType, offset, err = readByte(body, offset, "delivery channel type"); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	if env.FromUID, offset, err = readString(body, offset); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	if env.SenderNodeID, offset, err = readUvarint(body, offset); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	if env.SenderSessionID, offset, err = readUvarint(body, offset); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	if env.ClientMsgNo, offset, err = readString(body, offset); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	if redDot, offset, err = readByte(body, offset, "delivery red dot"); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	switch redDot {
	case 0:
		env.RedDot = false
	case 1:
		env.RedDot = true
	default:
		return channelappendcontract.CommittedEnvelope{}, offset, fmt.Errorf("internal/access/node: invalid delivery red dot flag")
	}
	if env.Payload, offset, err = readBytes(body, offset); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	if env.MessageScopedUIDs, offset, err = readStringSlice(body, offset, "delivery message scoped uids"); err != nil {
		return channelappendcontract.CommittedEnvelope{}, offset, err
	}
	return env, offset, nil
}

func appendDeliveryPushResult(dst []byte, result onlinedelivery.OwnerPushResult) []byte {
	dst = appendDeliveryEventRoutes(dst, result.Accepted)
	dst = appendDeliveryEventRoutes(dst, result.Retryable)
	return appendDeliveryEventRoutes(dst, result.Dropped)
}

func readDeliveryPushResult(body []byte, offset int) (onlinedelivery.OwnerPushResult, int, error) {
	var result onlinedelivery.OwnerPushResult
	var err error
	if result.Accepted, offset, err = readDeliveryEventRoutes(body, offset); err != nil {
		return onlinedelivery.OwnerPushResult{}, offset, err
	}
	if result.Retryable, offset, err = readDeliveryEventRoutes(body, offset); err != nil {
		return onlinedelivery.OwnerPushResult{}, offset, err
	}
	if result.Dropped, offset, err = readDeliveryEventRoutes(body, offset); err != nil {
		return onlinedelivery.OwnerPushResult{}, offset, err
	}
	return result, offset, nil
}

func appendBytes(dst []byte, value []byte) []byte {
	dst = appendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func readBytes(body []byte, offset int) ([]byte, int, error) {
	n, next, err := readUvarint(body, offset)
	if err != nil {
		return nil, offset, err
	}
	offset = next
	if n > uint64(len(body)-offset) {
		return nil, offset, fmt.Errorf("internal/access/node: short bytes")
	}
	if n == 0 {
		return nil, offset, nil
	}
	end := offset + int(n)
	return append([]byte(nil), body[offset:end]...), end, nil
}

func appendStringSlice(dst []byte, values []string) []byte {
	dst = appendUvarint(dst, uint64(len(values)))
	for _, value := range values {
		dst = appendString(dst, value)
	}
	return dst
}

func readStringSlice(body []byte, offset int, label string) ([]string, int, error) {
	count, next, err := readUvarint(body, offset)
	if err != nil {
		return nil, offset, err
	}
	offset = next
	if count == 0 {
		return nil, offset, nil
	}
	if err := validateDeliveryCollectionLen(count, len(body)-offset, label); err != nil {
		return nil, offset, err
	}
	values := make([]string, 0, int(count))
	for i := uint64(0); i < count; i++ {
		var value string
		if value, offset, err = readString(body, offset); err != nil {
			return nil, offset, err
		}
		values = append(values, value)
	}
	return values, offset, nil
}

func validateDeliveryCollectionLen(count uint64, remaining int, label string) error {
	if count > uint64(remaining) {
		return fmt.Errorf("internal/access/node: %s length exceeds payload", label)
	}
	if count > maxDeliveryRPCCollectionLen {
		return fmt.Errorf("internal/access/node: %s length exceeds limit", label)
	}
	return nil
}
