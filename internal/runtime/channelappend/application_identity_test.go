package channelappend

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type applicationIDReaderFunc func([]byte) (string, error)

func (f applicationIDReaderFunc) ReadApplicationMessageID(payload []byte) (string, error) {
	return f(payload)
}

func TestApplicationIdentityUsesActualCommittedBytesAndRetainsPayload(t *testing.T) {
	item := preparedSend{Command: SendCommand{
		FromUID: "sender", ChannelID: "room", ChannelType: 2, ClientMsgNo: "client-message-0001",
		Payload: []byte("this-attempt"), ApplicationAdmission: true,
	}}
	request := appendRequest(AuthorityTarget{ChannelID: ChannelID{ID: "room", Type: 2}}, []preparedSend{item}, 1)
	require.False(t, request.OmitResultPayload)
	result := AppendBatchResult{Items: []AppendBatchItemResult{{MessageID: 42, MessageSeq: 7, Message: Message{Payload: []byte("actual-old-commit"), ServerTimestampMS: 1788364800123}}}}
	ids := applicationIDReaderFunc(func(payload []byte) (string, error) {
		require.Equal(t, "actual-old-commit", string(payload))
		return "actual-public-id", nil
	})
	completions := appendResultCompletions([]preparedSend{item}, result, ids)
	require.Len(t, completions, 1)
	require.NoError(t, completions[0].result.Err)
	require.True(t, completions[0].committed)
	require.Equal(t, "actual-public-id", completions[0].result.Result.ApplicationMessageID)
	require.Equal(t, uint64(42), completions[0].result.Result.MessageID)
	require.Equal(t, int64(1788364800123), completions[0].result.Result.ServerTimestampMS)
}

func TestApplicationIdentityNeverFallsBackToUncommittedAttempt(t *testing.T) {
	item := preparedSend{Command: SendCommand{Payload: []byte("uncommitted-public-id"), ApplicationAdmission: true}}
	result := AppendBatchResult{Items: []AppendBatchItemResult{{MessageID: 42, MessageSeq: 7, Message: Message{ServerTimestampMS: 1788364800123}}}}
	ids := applicationIDReaderFunc(func(payload []byte) (string, error) {
		require.Empty(t, payload)
		return "", errors.New("missing committed metadata")
	})
	for _, reader := range []applicationIDReaderFunc{ids, nil} {
		var completions []appendItemCompletion
		if reader == nil {
			completions = appendResultCompletions([]preparedSend{item}, result, nil)
		} else {
			completions = appendResultCompletions([]preparedSend{item}, result, reader)
		}
		require.Error(t, completions[0].result.Err)
		require.Empty(t, completions[0].result.Result.ApplicationMessageID)
		require.Equal(t, ReasonSystemError, completions[0].result.Result.Reason)
	}
}

type applicationIDStore struct{ result SendResult }

func (s applicationIDStore) LookupSend(_ context.Context, query IdempotencyQuery) (SendResult, bool, error) {
	if !query.ApplicationAdmission {
		return SendResult{}, false, errors.New("missing application admission proof")
	}
	return s.result, true, nil
}

func TestApplicationIdentitySurvivesPrepareAndAppendFailureRecovery(t *testing.T) {
	want := SendResult{MessageID: 42, MessageSeq: 7, ApplicationMessageID: "committed-public-id", ServerTimestampMS: 1788364800123, Reason: ReasonSuccess}
	cmd := SendCommand{FromUID: "sender", ChannelID: "room", ChannelType: 2, ClientMsgNo: "client-message-0001", Payload: []byte("canonical"), ApplicationAdmission: true}
	store := applicationIDStore{result: want}
	next, done := prepareSend(context.Background(), cmd, preparePorts{idempotency: store}, true)
	require.True(t, done)
	require.Equal(t, want, next.result)
	results, observation := appendBatchErrorCompletionsOrRecoveries(context.Background(), []preparedSend{{Command: cmd}}, ErrAppendFailed, appendPorts{idempotency: store})
	require.Equal(t, 1, observation.RecoveredItems)
	require.Equal(t, want, results[0].result.Result)
}
