package channel

import (
	"context"
	"errors"
	"strings"
)

var (
	// ErrCommittedMessagesUnavailable reports an authority or storage failure.
	ErrCommittedMessagesUnavailable = errors.New("channel committed messages unavailable")
	// ErrCommittedMessagesQuery reports an invalid internal scan request.
	ErrCommittedMessagesQuery = errors.New("invalid committed messages query")
)

// MaxCommittedMessagesPageLimit is the service-only recovery page limit.
const MaxCommittedMessagesPageLimit = 100

// CommittedMessagesQuery selects one page in a committed-head snapshot.
type CommittedMessagesQuery struct {
	AfterMessageSeq uint64
	Limit           int
	ScanHead        uint64
}

// CommittedMessagesPage contains one ordered recovery page.
type CommittedMessagesPage struct {
	Messages                 []CommittedMessage
	ScanHead                 uint64
	FirstAvailableMessageSeq uint64
	NextAfterMessageSeq      uint64
	RetentionGap             bool
	HasMore                  bool
}

// CommittedMessagesReader reads one service-only Channel recovery page.
type CommittedMessagesReader interface {
	ReadCommittedMessages(context.Context, ChannelKey, CommittedMessagesQuery) (CommittedMessagesPage, bool, error)
}

// ReadCommittedMessages validates and delegates one internal recovery scan page.
func (a *App) ReadCommittedMessages(ctx context.Context, key ChannelKey, query CommittedMessagesQuery) (CommittedMessagesPage, bool, error) {
	if key.ChannelID == "" || strings.TrimSpace(key.ChannelID) != key.ChannelID || key.ChannelType == 0 ||
		query.Limit < 1 || query.Limit > MaxCommittedMessagesPageLimit || (query.ScanHead > 0 && query.AfterMessageSeq > query.ScanHead) {
		return CommittedMessagesPage{}, false, ErrCommittedMessagesQuery
	}
	if a == nil {
		return CommittedMessagesPage{}, false, ErrCommittedMessagesUnavailable
	}
	reader, ok := a.store.(CommittedMessagesReader)
	if !ok {
		return CommittedMessagesPage{}, false, ErrCommittedMessagesUnavailable
	}
	page, found, err := reader.ReadCommittedMessages(ctx, key, query)
	for index := range page.Messages {
		page.Messages[index].Payload = append([]byte(nil), page.Messages[index].Payload...)
	}
	return page, found, err
}
