package presence

import "context"

// LocalRegistry stores owner-local route projections and local session records.
type LocalRegistry interface {
	RegisterPending(LocalSession) error
	MarkActive(sessionID uint64) error
	MarkClosingAndUnregister(sessionID uint64) (OwnerRoute, bool)
	MarkTouched(sessionID uint64, activityUnix int64) (OwnerRoute, bool)
	MarkLeaseObserved(sessionID uint64, observedUnix int64) (OwnerRoute, bool)
	LocalSession(sessionID uint64) (LocalSession, bool)
	LocalSessionsByUID(uid string) []LocalSession
}

// AuthorityClient routes presence operations to the current UID authority.
type AuthorityClient interface {
	RegisterRoute(context.Context, Route) (RegisterResult, error)
	CommitRoute(context.Context, PendingRouteToken) error
	AbortRoute(context.Context, PendingRouteToken) error
	EnqueueUnregister(context.Context, RouteIdentity, uint64)
	EndpointsByUID(context.Context, string) ([]Route, error)
}

// OwnerActionClient applies conflict actions on the node that owns a real session.
type OwnerActionClient interface {
	ApplyRouteAction(context.Context, RouteAction) error
}

// LeaseEvent describes one installation-level connection lease transition.
type LeaseEvent struct {
	PrincipalUID           string `json:"principal_uid"`
	TransportUID           string `json:"transport_uid"`
	AppInstanceID          string `json:"app_instance_id"`
	DeviceSessionID        string `json:"device_session_id"`
	IMSessionID            string `json:"im_session_id"`
	InstallationGeneration uint64 `json:"installation_generation"`
	DeviceID               string `json:"device_id"`
	DeviceClass            string `json:"device_class"`
	DeviceFlag             uint8  `json:"device_flag"`
	SessionGeneration      uint64 `json:"session_generation"`
	AuthorizationFence     uint64 `json:"authorization_fence"`
	OwnerNodeID            uint64 `json:"owner_node_id"`
	OwnerBootID            uint64 `json:"owner_boot_id"`
	OwnerSeq               uint64 `json:"owner_seq"`
	SessionID              uint64 `json:"session_id"`
	ConnectionID           string `json:"connection_id"`
	Kind                   string `json:"kind"`
	ObservedAt             int64  `json:"observed_at"`
	ExpiresAt              int64  `json:"expires_at"`
}

// LeaseObserver receives best-effort owner-local installation lease changes.
type LeaseObserver interface {
	ObserveLease(context.Context, LeaseEvent) error
}

// EventPush describes one realtime WKProto EVENT fanout to active installations.
type EventPush struct {
	UIDs      []string
	EventID   string
	EventType string
	Timestamp uint64
	Payload   []byte
}

// EventPusher fans one event out through exact presence routes.
type EventPusher interface {
	PushEvent(context.Context, EventPush) error
}
