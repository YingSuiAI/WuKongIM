package node

import (
	"context"
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/contracts/onlinedelivery"
)

func TestDeliveryRPCHandlerDispatchesFenceCompletePush(t *testing.T) {
	push := testDeliveryPush()
	result := onlinedelivery.OwnerPushResult{
		Accepted: []onlinedelivery.Route{push.Routes[0]}, Retryable: []onlinedelivery.Route{}, Dropped: []onlinedelivery.Route{},
	}
	delivery := &fakeDeliveryOwnerPush{result: result}
	adapter := New(Options{Delivery: delivery})
	body, err := encodeDeliveryPushRequest(deliveryPushRequest{Push: push})
	if err != nil {
		t.Fatalf("encodeDeliveryPushRequest() error = %v", err)
	}
	responseBody, err := adapter.HandleDeliveryPushRPC(context.Background(), body)
	if err != nil {
		t.Fatalf("HandleDeliveryPushRPC() error = %v", err)
	}
	response, err := decodeDeliveryPushResponse(responseBody)
	if err != nil {
		t.Fatalf("decodeDeliveryPushResponse() error = %v", err)
	}
	if response.Status != rpcStatusOK || !reflect.DeepEqual(response.Result, result) {
		t.Fatalf("response = %#v, want status=%q result=%#v", response, rpcStatusOK, result)
	}
	if len(delivery.pushes) != 1 || !reflect.DeepEqual(delivery.pushes[0], push) {
		t.Fatalf("delivery pushes = %#v, want %#v", delivery.pushes, push)
	}
}

func TestDeliveryRPCHandlerRejectsNilDelivery(t *testing.T) {
	body, err := encodeDeliveryPushRequest(deliveryPushRequest{Push: testDeliveryPush()})
	if err != nil {
		t.Fatalf("encodeDeliveryPushRequest() error = %v", err)
	}
	responseBody, err := New(Options{}).HandleDeliveryPushRPC(context.Background(), body)
	if err != nil {
		t.Fatalf("HandleDeliveryPushRPC() error = %v", err)
	}
	response, err := decodeDeliveryPushResponse(responseBody)
	if err != nil {
		t.Fatalf("decodeDeliveryPushResponse() error = %v", err)
	}
	if response.Status != rpcStatusRejected {
		t.Fatalf("response status = %q, want %q", response.Status, rpcStatusRejected)
	}
}

func TestDeliveryClientUsesSingleDeliveryServiceAndPreservesRouteFences(t *testing.T) {
	push := testDeliveryPush()
	result := onlinedelivery.OwnerPushResult{
		Accepted: []onlinedelivery.Route{}, Retryable: append([]onlinedelivery.Route(nil), push.Routes...), Dropped: []onlinedelivery.Route{},
	}
	node := &fakeDeliveryRPCNode{response: deliveryPushResponse{Status: rpcStatusOK, Result: result}}
	got, err := NewClient(node).PushOwner(context.Background(), push)
	if err != nil {
		t.Fatalf("PushOwner() error = %v", err)
	}
	if node.nodeID != push.OwnerNodeID || node.serviceID != DeliveryPushRPCServiceID {
		t.Fatalf("RPC target = node %d service %d, want node %d service %d",
			node.nodeID, node.serviceID, push.OwnerNodeID, DeliveryPushRPCServiceID)
	}
	request, err := decodeDeliveryPushRequest(node.payload)
	if err != nil {
		t.Fatalf("decodeDeliveryPushRequest(client payload) error = %v", err)
	}
	if !reflect.DeepEqual(request.Push, push) {
		t.Fatalf("wire push = %#v, want %#v", request.Push, push)
	}
	if !reflect.DeepEqual(got, result) {
		t.Fatalf("PushOwner() = %#v, want %#v", got, result)
	}
}

func TestDeliveryClientMapsRejectedStatusToError(t *testing.T) {
	client := NewClient(&fakeDeliveryRPCNode{response: deliveryPushResponse{Status: rpcStatusRejected}})
	if _, err := client.PushOwner(context.Background(), testDeliveryPush()); err == nil {
		t.Fatal("PushOwner() error = nil, want rejected error")
	}
}

type fakeDeliveryOwnerPush struct {
	result onlinedelivery.OwnerPushResult
	err    error
	pushes []onlinedelivery.OwnerPush
}

func (f *fakeDeliveryOwnerPush) PushOwner(_ context.Context, push onlinedelivery.OwnerPush) (onlinedelivery.OwnerPushResult, error) {
	f.pushes = append(f.pushes, push.Clone())
	if f.err != nil {
		return onlinedelivery.OwnerPushResult{}, f.err
	}
	return f.result, nil
}

type fakeDeliveryRPCNode struct {
	response  deliveryPushResponse
	err       error
	nodeID    uint64
	serviceID uint8
	payload   []byte
}

func (f *fakeDeliveryRPCNode) CallRPC(_ context.Context, nodeID uint64, serviceID uint8, payload []byte) ([]byte, error) {
	f.nodeID = nodeID
	f.serviceID = serviceID
	f.payload = append([]byte(nil), payload...)
	if f.err != nil {
		return nil, f.err
	}
	return encodeDeliveryPushResponse(f.response)
}
