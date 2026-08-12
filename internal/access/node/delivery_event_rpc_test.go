package node

import (
	"context"
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/contracts/onlinedelivery"
)

func TestDeliveryEventRPCCodecAndOwnerPush(t *testing.T) {
	push := onlinedelivery.EventPush{
		OwnerNodeID: 2, EventID: "evt-1", EventType: "agent.delta", Timestamp: 99,
		Payload: []byte(`{"delta":"hi"}`), Routes: []onlinedelivery.Route{{UID: "u1", OwnerNodeID: 2, OwnerBootID: 3, OwnerSeq: 4, SessionID: 5, DeviceID: "device-1", AppInstanceID: "install-1", SessionGeneration: 9, ProtocolVersion: 6, DeviceFlag: 1, DeviceLevel: 1}},
	}
	handler := &recordingDeliveryEventHandler{}
	adapter := New(Options{DeliveryEvent: handler})
	body, err := encodeDeliveryEventPushRequest(deliveryEventPushRequest{Push: push})
	if err != nil {
		t.Fatal(err)
	}
	response, err := adapter.HandleDeliveryEventPushRPC(context.Background(), body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeDeliveryEventPushResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Status != rpcStatusOK || !reflect.DeepEqual(handler.push, push) {
		t.Fatalf("status=%q push=%#v, want exact EVENT push", decoded.Status, handler.push)
	}
}

type recordingDeliveryEventHandler struct{ push onlinedelivery.EventPush }

func (h *recordingDeliveryEventHandler) PushEventOwner(_ context.Context, push onlinedelivery.EventPush) (onlinedelivery.OwnerPushResult, error) {
	h.push = push.Clone()
	return onlinedelivery.OwnerPushResult{Accepted: append([]onlinedelivery.Route(nil), push.Routes...)}, nil
}
