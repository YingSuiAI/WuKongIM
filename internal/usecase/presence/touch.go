package presence

import "context"

// Touch records owner-local session activity for later batched authority updates.
func (a *App) Touch(ctx context.Context, cmd TouchCommand) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.local == nil {
		return ErrLocalRegistryUnavailable
	}
	route, ok := a.local.MarkTouched(cmd.SessionID, cmd.ActivityUnix)
	if ok && cmd.ActivityUnix-route.LastLeaseObservedUnix >= leaseHeartbeatSeconds {
		if route, ok = a.local.MarkLeaseObserved(cmd.SessionID, cmd.ActivityUnix); ok {
			a.observeLease(ctx, route, "heartbeat", cmd.ActivityUnix)
		}
	}
	return nil
}
