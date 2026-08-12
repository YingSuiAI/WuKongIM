package app

import (
	"context"
	"encoding/json"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/contracts/onlinedelivery"
	runtimedelivery "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	channelusecase "github.com/WuKongIM/WuKongIM/internal/usecase/channel"
	messageusecase "github.com/WuKongIM/WuKongIM/internal/usecase/message"
	presenceusecase "github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/protocol/channelid"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

type messageEventAPIFacade struct {
	*messageusecase.App
	delivery    messageEventDelivery
	presence    messageEventPresence
	subscribers messageEventSubscriberPager
	logger      wklog.Logger
}

type messageEventDelivery interface {
	PushEvent(context.Context, onlinedelivery.EventPush) (onlinedelivery.OwnerPushResult, error)
}

type messageEventPresence interface {
	EndpointsByUIDs(context.Context, []string) (map[string][]presenceusecase.Route, error)
}

type messageEventSubscriberPager interface {
	ListSubscribersPage(context.Context, channelusecase.MemberListPageRequest) (channelusecase.MemberListPageResult, error)
}

type realtimeMessageEventPayload struct {
	ChannelID   string          `json:"channel_id"`
	ChannelType int64           `json:"channel_type"`
	MessageID   uint64          `json:"message_id"`
	ClientMsgNo string          `json:"client_msg_no"`
	RunID       string          `json:"run_id"`
	EventType   string          `json:"event_type"`
	EventKey    string          `json:"event_key"`
	MsgEventSeq uint64          `json:"msg_event_seq"`
	Payload     json.RawMessage `json:"payload"`
}

func (f *messageEventAPIFacade) AppendMessageEvent(ctx context.Context, event messageusecase.MessageEventAppend) (messageusecase.MessageEventAppendResult, error) {
	result, err := f.App.AppendMessageEvent(ctx, event)
	if err != nil {
		return result, err
	}
	if !result.Applied {
		return result, nil
	}
	if f == nil || f.delivery == nil || f.presence == nil {
		return result, nil
	}
	payload, err := marshalRealtimeMessageEvent(event, result)
	if err != nil {
		return result, nil
	}
	timestamp := event.OccurredAt
	if timestamp <= 0 {
		timestamp = time.Now().UnixMilli()
	}
	if err := f.visitRecipientUIDPages(ctx, result.ChannelID, result.ChannelType, func(uids []string) error {
		return f.pushMessageEventPage(ctx, event, result, payload, uint64(timestamp), uids)
	}); err != nil && f.logger != nil {
		f.logger.Warn("message event realtime projection failed",
			wklog.String("event", "internal.app.message_event_projection_failed"), wklog.Error(err))
	}
	return result, nil
}

func (f *messageEventAPIFacade) pushMessageEventPage(ctx context.Context, event messageusecase.MessageEventAppend, result messageusecase.MessageEventAppendResult, payload []byte, timestamp uint64, uids []string) error {
	routesByUID, err := f.presence.EndpointsByUIDs(ctx, uids)
	if err != nil {
		return err
	}
	const routeBatchSize = 256
	byOwner := make(map[uint64][]onlinedelivery.Route)
	flush := func(ownerNodeID uint64) error {
		routes := byOwner[ownerNodeID]
		if len(routes) == 0 {
			return nil
		}
		deliveryResult, pushErr := f.delivery.PushEvent(ctx, onlinedelivery.EventPush{
			OwnerNodeID: ownerNodeID, EventID: result.EventID, EventType: event.EventType,
			Timestamp: timestamp, Payload: payload, Routes: routes,
		})
		byOwner[ownerNodeID] = byOwner[ownerNodeID][:0]
		if pushErr != nil {
			return pushErr
		}
		if len(deliveryResult.Retryable) > 0 {
			return runtimedelivery.ErrOwnerPushRetryExhausted
		}
		return nil
	}
	for _, uid := range uids {
		for _, route := range routesByUID[uid] {
			if route.OwnerNodeID == 0 || route.ProtocolVersion < uint8(frame.LatestVersion) {
				continue
			}
			ownerNodeID := route.OwnerNodeID
			byOwner[ownerNodeID] = append(byOwner[ownerNodeID], onlinedelivery.Route{
				UID: route.UID, OwnerNodeID: route.OwnerNodeID, OwnerBootID: route.OwnerBootID,
				OwnerSeq: route.OwnerSeq, SessionID: route.SessionID, DeviceID: route.DeviceID,
				AppInstanceID: route.AppInstanceID, InstallationGeneration: route.InstallationGeneration,
				SessionGeneration: route.SessionGeneration, AuthorizationFence: route.AuthorizationFence,
				ProtocolVersion: route.ProtocolVersion,
				DeviceFlag:      route.DeviceFlag, DeviceLevel: route.DeviceLevel,
			})
			if len(byOwner[ownerNodeID]) == routeBatchSize {
				if err := flush(ownerNodeID); err != nil {
					return err
				}
			}
		}
	}
	for ownerNodeID := range byOwner {
		if err := flush(ownerNodeID); err != nil {
			return err
		}
	}
	return nil
}

func marshalRealtimeMessageEvent(event messageusecase.MessageEventAppend, result messageusecase.MessageEventAppendResult) ([]byte, error) {
	return json.Marshal(realtimeMessageEventPayload{
		ChannelID: result.ChannelID, ChannelType: result.ChannelType, MessageID: result.MessageID,
		ClientMsgNo: result.ClientMsgNo, RunID: result.RunID, EventType: event.EventType,
		EventKey: result.EventKey, MsgEventSeq: result.MsgEventSeq,
		Payload: append(json.RawMessage(nil), event.Payload...),
	})
}

func (f *messageEventAPIFacade) visitRecipientUIDPages(ctx context.Context, channelID string, channelType int64, visit func([]string) error) error {
	switch uint8(channelType) {
	case frame.ChannelTypePerson:
		left, right, err := channelruntime.DecodePersonChannel(channelID)
		if err != nil {
			return err
		}
		return visit([]string{left, right})
	case frame.ChannelTypeAgent:
		uid, _, err := channelruntime.DecodeAgentChannel(channelID)
		if err != nil {
			return err
		}
		return visit([]string{uid})
	default:
		if f.subscribers == nil {
			return nil
		}
		cursor := ""
		for {
			page, err := f.subscribers.ListSubscribersPage(ctx, channelusecase.MemberListPageRequest{ChannelKey: channelusecase.ChannelKey{ChannelID: channelID, ChannelType: uint8(channelType)}, AfterUID: cursor, Limit: 512})
			if err != nil {
				return err
			}
			uids := make([]string, 0, len(page.Members))
			for _, member := range page.Members {
				uids = append(uids, member.UID)
			}
			if len(uids) > 0 {
				if err := visit(uids); err != nil {
					return err
				}
			}
			if !page.HasMore {
				return nil
			}
			cursor = page.NextCursor
		}
	}
}
