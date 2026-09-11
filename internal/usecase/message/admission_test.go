package message

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type admissionFunc func(context.Context, SendCommand) ([]byte, Reason, error)

func (f admissionFunc) AdmitSend(ctx context.Context, cmd SendCommand) ([]byte, Reason, error) {
	return f(ctx, cmd)
}

type admissionHookFunc func(context.Context, SendCommand) (SendCommand, Reason, error)

func (f admissionHookFunc) BeforeSend(ctx context.Context, cmd SendCommand) (SendCommand, Reason, error) {
	return f(ctx, cmd)
}

type admissionRecordingSubmitter struct {
	mu       sync.Mutex
	commands []SendCommand
}

func (s *admissionRecordingSubmitter) Send(_ context.Context, cmd SendCommand) (SendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, cmd)
	return SendResult{MessageID: 42, MessageSeq: 7, Reason: ReasonSuccess}, nil
}

func (s *admissionRecordingSubmitter) SendBatch(items []SendBatchItem) []SendBatchItemResult {
	results := make([]SendBatchItemResult, len(items))
	for i, item := range items {
		results[i].Result, results[i].Err = s.Send(item.Context, item.Command)
	}
	return results
}

func TestMandatoryAdmissionRunsAfterPluginsAcrossSendEntries(t *testing.T) {
	for _, count := range []int{0, 1, 2} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%d/skip_plugins=%v", count, skip), func(t *testing.T) {
				submitter := &admissionRecordingSubmitter{}
				app := New(Options{Submitter: submitter,
					SendHook: admissionHookFunc(func(_ context.Context, cmd SendCommand) (SendCommand, Reason, error) {
						cmd.Payload = []byte("plugin-output")
						return cmd, ReasonSuccess, nil
					}),
					SendAdmission: admissionFunc(func(_ context.Context, cmd SendCommand) ([]byte, Reason, error) {
						want := "plugin-output"
						if skip {
							want = "original"
						}
						if string(cmd.Payload) != want {
							return nil, ReasonSystemError, fmt.Errorf("payload = %q", cmd.Payload)
						}
						return []byte("canonical"), ReasonSuccess, nil
					}),
				})
				cmd := SendCommand{FromUID: "sender", ChannelID: "room", ChannelType: 2,
					Payload: []byte("original"), SkipPluginHooks: skip, SenderSessionID: 1}
				if count == 0 {
					_, err := app.Send(context.Background(), cmd)
					require.NoError(t, err)
				} else {
					items := make([]SendBatchItem, count)
					for i := range items {
						items[i].Command = cmd
					}
					for _, result := range app.SendBatch(items) {
						require.NoError(t, result.Err)
						require.Equal(t, ReasonSuccess, result.Result.Reason)
					}
				}
				require.NotEmpty(t, submitter.commands)
				for _, submitted := range submitter.commands {
					require.Equal(t, "canonical", string(submitted.Payload))
					require.True(t, submitted.ApplicationAdmission)
				}
			})
		}
	}
}

func TestMandatoryAdmissionFailureNeverSubmits(t *testing.T) {
	failure := errors.New("admission unavailable")
	submitter := &admissionRecordingSubmitter{}
	app := New(Options{Submitter: submitter, SendAdmission: admissionFunc(func(context.Context, SendCommand) ([]byte, Reason, error) {
		return nil, ReasonSystemError, failure
	})})
	cmd := SendCommand{FromUID: "sender", ChannelID: "room", ChannelType: 2, Payload: []byte("message"), SkipPluginHooks: true}
	_, err := app.Send(context.Background(), cmd)
	require.ErrorIs(t, err, failure)
	for _, result := range app.SendBatch([]SendBatchItem{{Command: cmd}, {Command: cmd}}) {
		require.ErrorIs(t, result.Err, failure)
	}
	require.Empty(t, submitter.commands)
}

func TestMandatoryAdmissionDurabilityFlagsCannotSpoofServiceOrigin(t *testing.T) {
	for _, noPersist := range []bool{false, true} {
		for _, service := range []bool{false, true} {
			for _, device := range []bool{false, true} {
				t.Run(fmt.Sprintf("noPersist=%v/service=%v/device=%v", noPersist, service, device), func(t *testing.T) {
					submitter := &admissionRecordingSubmitter{}
					app := New(Options{Submitter: submitter, SendAdmission: admissionFunc(func(context.Context, SendCommand) ([]byte, Reason, error) {
						return nil, ReasonSystemError, errors.New("control must not call content admission")
					})})
					cmd := SendCommand{FromUID: "sender", ChannelID: "room", ChannelType: 2, Payload: []byte("control"),
						NoPersist: noPersist, SyncOnce: !noPersist, ServiceAuthenticated: service}
					if device {
						cmd.SenderSessionID = 1
					}
					result, err := app.Send(context.Background(), cmd)
					require.NoError(t, err)
					batch := app.SendBatch([]SendBatchItem{{Command: cmd}, {Command: cmd}})
					allowed := service && !device
					if allowed {
						require.Equal(t, ReasonSuccess, result.Reason)
						require.Len(t, submitter.commands, 3)
						for _, submitted := range submitter.commands {
							require.False(t, submitted.ApplicationAdmission)
						}
					} else {
						require.Equal(t, ReasonNotAllowSend, result.Reason)
						for _, item := range batch {
							require.Equal(t, ReasonNotAllowSend, item.Result.Reason)
						}
						require.Empty(t, submitter.commands)
					}
				})
			}
		}
	}
}
