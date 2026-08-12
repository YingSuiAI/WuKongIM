package node

import (
	"context"
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/contracts/onlinedelivery"
	clusternet "github.com/WuKongIM/WuKongIM/pkg/cluster/net"
)

// DeliveryEventPushRPCServiceID is the owner-node typed EVENT service.
const DeliveryEventPushRPCServiceID uint8 = clusternet.RPCDeliveryEventPush

// HandleDeliveryEventPushRPC writes one typed EVENT batch on its owner node.
func (a *Adapter) HandleDeliveryEventPushRPC(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := decodeDeliveryEventPushRequest(payload)
	if err != nil {
		return nil, err
	}
	if a == nil || a.deliveryEvent == nil {
		return encodeDeliveryEventPushResponse(deliveryEventPushResponse{Status: rpcStatusRejected})
	}
	result, err := a.deliveryEvent.PushEventOwner(ctx, req.Push)
	if err != nil {
		return encodeDeliveryEventPushResponse(deliveryEventPushResponse{Status: rpcStatusRejected})
	}
	return encodeDeliveryEventPushResponse(deliveryEventPushResponse{Status: rpcStatusOK, Result: result})
}

// PushEventOwner forwards one typed EVENT batch to its owner node.
func (c *Client) PushEventOwner(ctx context.Context, push onlinedelivery.EventPush) (onlinedelivery.OwnerPushResult, error) {
	if c == nil || c.node == nil {
		return onlinedelivery.OwnerPushResult{}, fmt.Errorf("internal/access/node: delivery event rpc client not configured")
	}
	body, err := encodeDeliveryEventPushRequest(deliveryEventPushRequest{Push: push.Clone()})
	if err != nil {
		return onlinedelivery.OwnerPushResult{}, err
	}
	response, err := c.node.CallRPC(ctx, push.OwnerNodeID, DeliveryEventPushRPCServiceID, body)
	if err != nil {
		return onlinedelivery.OwnerPushResult{}, err
	}
	decoded, err := decodeDeliveryEventPushResponse(response)
	if err != nil {
		return onlinedelivery.OwnerPushResult{}, err
	}
	if decoded.Status != rpcStatusOK {
		return onlinedelivery.OwnerPushResult{}, fmt.Errorf("internal/access/node: delivery event rpc rejected")
	}
	return decoded.Result, nil
}
