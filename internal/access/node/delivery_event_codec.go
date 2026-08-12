package node

import (
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/contracts/onlinedelivery"
)

var (
	deliveryEventRequestMagic  = [...]byte{'W', 'K', 'V', 'E', 1}
	deliveryEventResponseMagic = [...]byte{'W', 'K', 'V', 'e', 1}
)

type deliveryEventPushRequest struct{ Push onlinedelivery.EventPush }
type deliveryEventPushResponse struct {
	Status string
	Result onlinedelivery.OwnerPushResult
}

func encodeDeliveryEventPushRequest(req deliveryEventPushRequest) ([]byte, error) {
	dst := append([]byte(nil), deliveryEventRequestMagic[:]...)
	dst = appendUvarint(dst, req.Push.OwnerNodeID)
	dst = appendString(dst, req.Push.EventID)
	dst = appendString(dst, req.Push.EventType)
	dst = appendUvarint(dst, req.Push.Timestamp)
	dst = appendBytes(dst, req.Push.Payload)
	dst = appendUvarint(dst, uint64(len(req.Push.Routes)))
	for _, route := range req.Push.Routes {
		dst = appendDeliveryEventRoute(dst, route)
	}
	return dst, nil
}

func decodeDeliveryEventPushRequest(body []byte) (deliveryEventPushRequest, error) {
	if !hasMagic(body, deliveryEventRequestMagic[:]) {
		return deliveryEventPushRequest{}, fmt.Errorf("internal/access/node: invalid delivery event request codec")
	}
	offset := len(deliveryEventRequestMagic)
	var req deliveryEventPushRequest
	var err error
	if req.Push.OwnerNodeID, offset, err = readUvarint(body, offset); err != nil {
		return req, err
	}
	if req.Push.EventID, offset, err = readString(body, offset); err != nil {
		return req, err
	}
	if req.Push.EventType, offset, err = readString(body, offset); err != nil {
		return req, err
	}
	if req.Push.Timestamp, offset, err = readUvarint(body, offset); err != nil {
		return req, err
	}
	if req.Push.Payload, offset, err = readBytes(body, offset); err != nil {
		return req, err
	}
	count, next, err := readUvarint(body, offset)
	if err != nil {
		return req, err
	}
	offset = next
	if count > maxDeliveryRPCCollectionLen {
		return req, fmt.Errorf("internal/access/node: delivery event routes length exceeds limit")
	}
	routes := make([]onlinedelivery.Route, 0, int(count))
	for i := uint64(0); i < count; i++ {
		route, next, readErr := readDeliveryEventRoute(body, offset)
		if readErr != nil {
			return req, readErr
		}
		offset = next
		routes = append(routes, route)
	}
	if offset != len(body) {
		return req, fmt.Errorf("internal/access/node: trailing delivery event request bytes")
	}
	req.Push.Routes = routes
	return req, nil
}

func encodeDeliveryEventPushResponse(resp deliveryEventPushResponse) ([]byte, error) {
	dst := append([]byte(nil), deliveryEventResponseMagic[:]...)
	dst = appendString(dst, resp.Status)
	dst = appendDeliveryEventRoutes(dst, resp.Result.Accepted)
	dst = appendDeliveryEventRoutes(dst, resp.Result.Retryable)
	dst = appendDeliveryEventRoutes(dst, resp.Result.Dropped)
	return dst, nil
}

func decodeDeliveryEventPushResponse(body []byte) (deliveryEventPushResponse, error) {
	if !hasMagic(body, deliveryEventResponseMagic[:]) {
		return deliveryEventPushResponse{}, fmt.Errorf("internal/access/node: invalid delivery event response codec")
	}
	offset := len(deliveryEventResponseMagic)
	status, offset, err := readString(body, offset)
	if err != nil {
		return deliveryEventPushResponse{}, err
	}
	accepted, offset, err := readDeliveryEventRoutes(body, offset)
	if err != nil {
		return deliveryEventPushResponse{}, err
	}
	retryable, offset, err := readDeliveryEventRoutes(body, offset)
	if err != nil {
		return deliveryEventPushResponse{}, err
	}
	dropped, offset, err := readDeliveryEventRoutes(body, offset)
	if err != nil {
		return deliveryEventPushResponse{}, err
	}
	if offset != len(body) {
		return deliveryEventPushResponse{}, fmt.Errorf("internal/access/node: trailing delivery event response bytes")
	}
	return deliveryEventPushResponse{Status: status, Result: onlinedelivery.OwnerPushResult{Accepted: accepted, Retryable: retryable, Dropped: dropped}}, nil
}

func appendDeliveryEventRoutes(dst []byte, routes []onlinedelivery.Route) []byte {
	dst = appendUvarint(dst, uint64(len(routes)))
	for _, route := range routes {
		dst = appendDeliveryEventRoute(dst, route)
	}
	return dst
}

func readDeliveryEventRoutes(body []byte, offset int) ([]onlinedelivery.Route, int, error) {
	count, next, err := readUvarint(body, offset)
	if err != nil {
		return nil, offset, err
	}
	offset = next
	if count > maxDeliveryRPCCollectionLen {
		return nil, offset, fmt.Errorf("internal/access/node: delivery event routes length exceeds limit")
	}
	routes := make([]onlinedelivery.Route, 0, int(count))
	for i := uint64(0); i < count; i++ {
		route, next, err := readDeliveryEventRoute(body, offset)
		if err != nil {
			return nil, offset, err
		}
		offset = next
		routes = append(routes, route)
	}
	return routes, offset, nil
}

func appendDeliveryEventRoute(dst []byte, route onlinedelivery.Route) []byte {
	dst = appendString(dst, route.UID)
	dst = appendUvarint(dst, route.OwnerNodeID)
	dst = appendUvarint(dst, route.OwnerBootID)
	dst = appendUvarint(dst, route.OwnerSeq)
	dst = appendUvarint(dst, route.SessionID)
	dst = appendString(dst, route.DeviceID)
	dst = appendString(dst, route.AppInstanceID)
	dst = appendUvarint(dst, route.InstallationGeneration)
	dst = appendUvarint(dst, route.SessionGeneration)
	dst = appendUvarint(dst, route.AuthorizationFence)
	dst = append(dst, route.ProtocolVersion, route.DeviceFlag, route.DeviceLevel)
	return dst
}

func readDeliveryEventRoute(body []byte, offset int) (onlinedelivery.Route, int, error) {
	var route onlinedelivery.Route
	var err error
	if route.UID, offset, err = readString(body, offset); err != nil {
		return route, offset, err
	}
	if route.OwnerNodeID, offset, err = readUvarint(body, offset); err != nil {
		return route, offset, err
	}
	if route.OwnerBootID, offset, err = readUvarint(body, offset); err != nil {
		return route, offset, err
	}
	if route.OwnerSeq, offset, err = readUvarint(body, offset); err != nil {
		return route, offset, err
	}
	if route.SessionID, offset, err = readUvarint(body, offset); err != nil {
		return route, offset, err
	}
	if route.DeviceID, offset, err = readString(body, offset); err != nil {
		return route, offset, err
	}
	if route.AppInstanceID, offset, err = readString(body, offset); err != nil {
		return route, offset, err
	}
	if route.InstallationGeneration, offset, err = readUvarint(body, offset); err != nil {
		return route, offset, err
	}
	if route.SessionGeneration, offset, err = readUvarint(body, offset); err != nil {
		return route, offset, err
	}
	if route.AuthorizationFence, offset, err = readUvarint(body, offset); err != nil {
		return route, offset, err
	}
	if route.ProtocolVersion, offset, err = readByte(body, offset, "delivery event protocol version"); err != nil {
		return route, offset, err
	}
	if route.DeviceFlag, offset, err = readByte(body, offset, "delivery event device flag"); err != nil {
		return route, offset, err
	}
	if route.DeviceLevel, offset, err = readByte(body, offset, "delivery event device level"); err != nil {
		return route, offset, err
	}
	return route, offset, nil
}
