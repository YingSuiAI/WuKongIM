package controller

import (
	"context"
	"errors"
	"testing"
)

func TestSlotRequestsFailClosedBeforeControllerStarts(t *testing.T) {
	runtime := &Runtime{}
	operations := []struct {
		name string
		run  func(context.Context) error
	}{
		{name: "leader transfer", run: func(ctx context.Context) error {
			_, err := runtime.RequestSlotLeaderTransfer(ctx, SlotLeaderTransferRequest{})
			return err
		}},
		{name: "replica move", run: func(ctx context.Context) error {
			_, err := runtime.RequestSlotReplicaMove(ctx, SlotReplicaMoveRequest{})
			return err
		}},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.run(context.Background()); !errors.Is(err, ErrNotStarted) {
				t.Fatalf("before start error = %v, want ErrNotStarted", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := operation.run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled error = %v, want context.Canceled", err)
			}
		})
	}
}
