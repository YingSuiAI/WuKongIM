package channel

import (
	"context"
	"errors"
	"strings"
)

// ErrCommittedHeadUnavailable reports that the trusted metadata-only cutoff
// read cannot be completed.
var ErrCommittedHeadUnavailable = errors.New("channel committed head unavailable")

// CommittedHeadReader captures only committed metadata, without requiring a
// user membership and without reading message bodies or sender indexes.
type CommittedHeadReader interface {
	ReadCommittedHead(context.Context, ChannelKey) (uint64, error)
}

// ReadCommittedHead is a trusted-service cutoff read, not a content permission.
// Capturing a cutoff does not serialize a later external membership mutation.
func (a *App) ReadCommittedHead(ctx context.Context, key ChannelKey) (uint64, error) {
	if key.ChannelID == "" || strings.TrimSpace(key.ChannelID) != key.ChannelID || key.ChannelType == 0 {
		return 0, ErrCommittedHeadUnavailable
	}
	if a == nil {
		return 0, ErrCommittedHeadUnavailable
	}
	reader, ok := a.store.(CommittedHeadReader)
	if !ok {
		return 0, ErrCommittedHeadUnavailable
	}
	return reader.ReadCommittedHead(ctx, key)
}
