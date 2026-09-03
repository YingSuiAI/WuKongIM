package app

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/WuKongIM/WuKongIM/internal/runtime/channelappend"
	runtimewebhook "github.com/WuKongIM/WuKongIM/internal/runtime/webhook"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

type webhookEventRuntime interface {
	Notify(context.Context, runtimewebhook.Message)
	Offline(context.Context, runtimewebhook.OfflineMessage)
	PresenceLease(context.Context, runtimewebhook.PresenceLease)
}

type webhookNotifyEnqueuer struct {
	runtime webhookEventRuntime
}

type webhookOfflineObserver struct {
	runtime      webhookEventRuntime
	uidBatchSize int
}

type webhookPresenceObserver struct {
	runtime webhookEventRuntime
}

type webhookLogObserver struct {
	logger wklog.Logger
}

type composedPersistAfterEnqueuer struct {
	left  channelappend.PersistAfterEnqueuer
	right channelappend.PersistAfterEnqueuer
}

type composedOfflineRecipientsObserver struct {
	pluginSingle channelappend.OfflineRecipientObserver
	pluginBatch  channelappend.OfflineRecipientsObserver
	webhookBatch channelappend.OfflineRecipientsObserver
}

type singleOfflineRecipientsObserver struct {
	// next receives one compatibility callback per UID in a canonical batch.
	next channelappend.OfflineRecipientObserver
}

func (a *App) wireWebhook() error {
	if !a.cfg.Webhook.Enabled || a.webhook != nil {
		return nil
	}
	sender := runtimewebhook.NewHTTPSender(runtimewebhook.HTTPSenderOptions{
		Addr:          a.cfg.Webhook.HTTPAddr,
		Timeout:       a.cfg.Webhook.RequestTimeout,
		SigningSecret: a.cfg.Webhook.SigningSecret,
	})
	runtime, err := runtimewebhook.New(runtimewebhook.RuntimeOptions{
		Sender:              sender,
		QueueSize:           a.cfg.Webhook.QueueSize,
		Workers:             a.cfg.Webhook.Workers,
		NotifyBatchMaxItems: a.cfg.Webhook.NotifyBatchMaxItems,
		NotifyBatchMaxWait:  a.cfg.Webhook.NotifyBatchMaxWait,
		OnlineBatchMaxItems: a.cfg.Webhook.OnlineBatchMaxItems,
		OnlineBatchMaxWait:  a.cfg.Webhook.OnlineBatchMaxWait,
		OfflineUIDBatchSize: a.cfg.Webhook.OfflineUIDBatchSize,
		RequestTimeout:      a.cfg.Webhook.RequestTimeout,
		RetryMaxAttempts:    a.cfg.Webhook.RetryMaxAttempts,
		FocusEvents:         a.cfg.Webhook.FocusEvents,
		Goroutines:          a.goroutines,
		Observer:            webhookLogObserver{logger: a.logger.Named("webhook")},
	})
	if err != nil {
		return fmt.Errorf("internal/app: create webhook runtime: %w", err)
	}
	a.webhook = runtime
	a.webhookNotify = webhookNotifyEnqueuer{runtime: runtime}
	a.webhookOffline = webhookOfflineObserver{runtime: runtime, uidBatchSize: a.cfg.Webhook.OfflineUIDBatchSize}
	a.webhookPresence = webhookPresenceObserver{runtime: runtime}
	return nil
}

func (o webhookLogObserver) ObserveWebhook(observation runtimewebhook.Observation) {
	if o.logger == nil || (observation.Result != "retry" && observation.Result != "retry_exhausted") {
		return
	}
	fields := []wklog.Field{
		wklog.Event("internal.app.webhook." + observation.Result),
		wklog.String("webhookEvent", observation.Event),
		wklog.String("queue", observation.Queue),
		wklog.Result(observation.Result),
		wklog.Int("items", observation.Items),
		wklog.Attempt(observation.Attempt),
		wklog.Duration("duration", observation.Duration),
		wklog.ErrorCode(webhookFailureClass(observation.Err)),
	}
	if observation.RequestID != "" {
		fields = append(fields, wklog.RequestID(observation.RequestID))
	}
	if observation.ChannelID != "" {
		fields = append(fields, wklog.ChannelID(observation.ChannelID), wklog.ChannelType(int64(observation.ChannelType)))
	}
	if observation.MessageID != 0 {
		fields = append(fields, wklog.MessageID(int64(observation.MessageID)), wklog.MessageSeq(observation.MessageSeq))
	}
	if observation.Result == "retry_exhausted" {
		o.logger.Warn("webhook delivery retry exhausted; dropping in-memory batch", fields...)
		return
	}
	o.logger.Debug("webhook delivery attempt failed", fields...)
}

func webhookFailureClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	const statusPrefix = "webhook: http status "
	message := err.Error()
	if strings.HasPrefix(message, statusPrefix) {
		status := strings.TrimPrefix(message, statusPrefix)
		code, parseErr := strconv.Atoi(status)
		if parseErr == nil && code >= 100 && code <= 599 {
			return "http_" + status
		}
	}
	return "transport"
}

func (e webhookNotifyEnqueuer) EnqueuePersistAfter(ctx context.Context, event channelappend.CommittedEnvelope) {
	if e.runtime == nil {
		return
	}
	e.runtime.Notify(ctx, webhookMessageFromCommitted(event))
}

func (o webhookOfflineObserver) ObserveOfflineRecipients(ctx context.Context, event channelappend.OfflineRecipientsEvent) {
	if o.runtime == nil || len(event.UIDs) == 0 {
		return
	}
	batchSize := o.uidBatchSize
	if batchSize <= 0 || batchSize > len(event.UIDs) {
		batchSize = len(event.UIDs)
	}
	message := webhookMessageFromCommitted(event.Event)
	for start := 0; start < len(event.UIDs); start += batchSize {
		end := start + batchSize
		if end > len(event.UIDs) {
			end = len(event.UIDs)
		}
		o.runtime.Offline(ctx, runtimewebhook.OfflineMessage{
			Message: message,
			ToUIDs:  append([]string(nil), event.UIDs[start:end]...),
		})
	}
}

func (o webhookOfflineObserver) ObserveOfflineRecipient(ctx context.Context, event channelappend.OfflineRecipientEvent) {
	if event.UID == "" {
		return
	}
	o.ObserveOfflineRecipients(ctx, channelappend.OfflineRecipientsEvent{
		Event: event.Event,
		UIDs:  []string{event.UID},
	})
}

func (o webhookPresenceObserver) ObserveLease(ctx context.Context, event presence.LeaseEvent) error {
	if o.runtime == nil {
		return nil
	}
	o.runtime.PresenceLease(ctx, runtimewebhook.PresenceLease(event))
	return nil
}

func composePersistAfterEnqueuers(left, right channelappend.PersistAfterEnqueuer) channelappend.PersistAfterEnqueuer {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return composedPersistAfterEnqueuer{left: left, right: right}
}

func (e composedPersistAfterEnqueuer) EnqueuePersistAfter(ctx context.Context, event channelappend.CommittedEnvelope) {
	e.left.EnqueuePersistAfter(ctx, event)
	e.right.EnqueuePersistAfter(ctx, event)
}

func composeOfflineRecipientObservers(
	pluginSingle channelappend.OfflineRecipientObserver,
	webhookBatch channelappend.OfflineRecipientsObserver,
) channelappend.OfflineRecipientsObserver {
	if pluginSingle == nil {
		return webhookBatch
	}
	pluginBatch, _ := pluginSingle.(channelappend.OfflineRecipientsObserver)
	if webhookBatch == nil {
		if pluginBatch != nil {
			return pluginBatch
		}
		return singleOfflineRecipientsObserver{next: pluginSingle}
	}
	return composedOfflineRecipientsObserver{
		pluginSingle: pluginSingle,
		pluginBatch:  pluginBatch,
		webhookBatch: webhookBatch,
	}
}

func (o singleOfflineRecipientsObserver) ObserveOfflineRecipients(ctx context.Context, event channelappend.OfflineRecipientsEvent) {
	for _, uid := range event.UIDs {
		o.next.ObserveOfflineRecipient(ctx, channelappend.OfflineRecipientEvent{Event: event.Event, UID: uid})
	}
}

func (o composedOfflineRecipientsObserver) ObserveOfflineRecipients(ctx context.Context, event channelappend.OfflineRecipientsEvent) {
	o.webhookBatch.ObserveOfflineRecipients(ctx, event)
	if o.pluginBatch != nil {
		o.pluginBatch.ObserveOfflineRecipients(ctx, event)
		return
	}
	for _, uid := range event.UIDs {
		o.pluginSingle.ObserveOfflineRecipient(ctx, channelappend.OfflineRecipientEvent{
			Event: event.Event,
			UID:   uid,
		})
	}
}

func webhookMessageFromCommitted(event channelappend.CommittedEnvelope) runtimewebhook.Message {
	return runtimewebhook.Message{
		MessageID:         event.MessageID,
		MessageSeq:        event.MessageSeq,
		ChannelID:         event.ChannelID,
		ChannelType:       event.ChannelType,
		Setting:           event.Setting,
		Topic:             event.Topic,
		Expire:            event.Expire,
		SourceID:          event.SenderNodeID,
		FromUID:           event.FromUID,
		ClientMsgNo:       event.ClientMsgNo,
		ServerTimestampMS: event.ServerTimestampMS,
		Payload:           append([]byte(nil), event.Payload...),
		RedDot:            event.RedDot,
		SyncOnce:          event.SyncOnce,
	}
}
