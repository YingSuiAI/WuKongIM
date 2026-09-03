package channel

import (
	"context"
	"errors"
	"strings"
)

var (
	// ErrCommittedMessageUnavailable reports an authority or storage failure.
	ErrCommittedMessageUnavailable = errors.New("channel committed message unavailable")
	// ErrCommittedMessageIdentity reports an invalid exact proof identity.
	ErrCommittedMessageIdentity = errors.New("invalid committed message identity")
)

// CommittedMessageIdentity selects one provider message tuple.
type CommittedMessageIdentity struct {
	MessageID  uint64
	MessageSeq uint64
}

// CommittedMessage is the immutable durable tuple returned by an exact proof.
type CommittedMessage struct {
	MessageID         uint64
	MessageSeq        uint64
	ChannelID         string
	ChannelType       uint8
	Setting           uint8
	FromUID           string
	ClientMsgNo       string
	ServerTimestampMS int64
	SyncOnce          bool
	Payload           []byte
}

// CommittedMessageReader performs one exact service-only proof read.
type CommittedMessageReader interface {
	ReadCommittedMessage(context.Context, ChannelKey, CommittedMessageIdentity) (CommittedMessage, bool, error)
}

// ReadCommittedMessage verifies one immutable provider tuple without current membership.
func (a *App) ReadCommittedMessage(ctx context.Context, key ChannelKey, identity CommittedMessageIdentity) (CommittedMessage, bool, error) {
	if key.ChannelID == "" || strings.TrimSpace(key.ChannelID) != key.ChannelID || key.ChannelType == 0 || identity.MessageID == 0 || identity.MessageSeq == 0 {
		return CommittedMessage{}, false, ErrCommittedMessageIdentity
	}
	if a == nil {
		return CommittedMessage{}, false, ErrCommittedMessageUnavailable
	}
	reader, ok := a.store.(CommittedMessageReader)
	if !ok {
		return CommittedMessage{}, false, ErrCommittedMessageUnavailable
	}
	message, found, err := reader.ReadCommittedMessage(ctx, key, identity)
	message.Payload = append([]byte(nil), message.Payload...)
	return message, found, err
}
