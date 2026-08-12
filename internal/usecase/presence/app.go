package presence

import (
	"context"
	"strconv"
)

const (
	leaseHeartbeatSeconds = int64(30)
	leaseTTLSeconds       = int64(90)
)

// HashSlotResolver resolves the current hash slot for a UID.
type HashSlotResolver func(uid string) (uint16, error)

// OwnerSeqGenerator allocates the owner-local fencing sequence for a route.
type OwnerSeqGenerator func(uid string) uint64

// Options configures the presence usecase.
type Options struct {
	// Local stores owner-local route projections and local session records.
	Local LocalRegistry
	// Authority registers virtual routes with the UID authority.
	Authority AuthorityClient
	// OwnerActions routes conflict actions to the owner node of the real session.
	OwnerActions OwnerActionClient
	// LeaseObserver observes best-effort installation-level lease transitions.
	LeaseObserver LeaseObserver
	// OwnerNodeID identifies this gateway owner node in authority route identities.
	OwnerNodeID uint64
	// OwnerBootID identifies this owner process generation.
	OwnerBootID uint64
	// HashSlot resolves UID hash slots for owner-local route metadata.
	HashSlot HashSlotResolver
	// OwnerSeq allocates monotonic owner sequences; nil falls back to SessionID.
	OwnerSeq OwnerSeqGenerator
}

// App orchestrates entry-agnostic presence activation and lookup.
type App struct {
	local         LocalRegistry
	authority     AuthorityClient
	ownerAction   OwnerActionClient
	leaseObserver LeaseObserver
	ownerNodeID   uint64
	ownerBootID   uint64
	hashSlot      HashSlotResolver
	ownerSeq      OwnerSeqGenerator
}

// New creates a presence App.
func New(opts Options) *App {
	return &App{
		local:         opts.Local,
		authority:     opts.Authority,
		ownerAction:   opts.OwnerActions,
		leaseObserver: opts.LeaseObserver,
		ownerNodeID:   opts.OwnerNodeID,
		ownerBootID:   opts.OwnerBootID,
		hashSlot:      opts.HashSlot,
		ownerSeq:      opts.OwnerSeq,
	}
}

func (a *App) observeLease(ctx context.Context, route OwnerRoute, kind string, observedUnix int64) {
	if a.leaseObserver == nil || route.UID == "" {
		return
	}
	expiresAt := observedUnix
	if kind != "disconnected" {
		expiresAt += leaseTTLSeconds
	}
	_ = a.leaseObserver.ObserveLease(ctx, LeaseEvent{
		PrincipalUID: route.UID, TransportUID: route.UID,
		AppInstanceID: route.AppInstanceID, DeviceID: route.DeviceID,
		DeviceSessionID: route.DeviceSessionID, IMSessionID: route.IMSessionID,
		InstallationGeneration: route.InstallationGeneration,
		DeviceClass:            deviceClass(route.DeviceFlag), DeviceFlag: route.DeviceFlag,
		SessionGeneration:  route.SessionGeneration,
		AuthorizationFence: route.AuthorizationFence,
		OwnerNodeID:        route.OwnerNodeID, OwnerBootID: route.OwnerBootID, OwnerSeq: route.OwnerSeq,
		SessionID: route.SessionID, ConnectionID: connectionID(route), Kind: kind,
		ObservedAt: observedUnix * 1000, ExpiresAt: expiresAt * 1000,
	})
}

func connectionID(route OwnerRoute) string {
	return strconv.FormatUint(route.OwnerNodeID, 10) + ":" + strconv.FormatUint(route.OwnerBootID, 10) + ":" + strconv.FormatUint(route.SessionID, 10)
}

func deviceClass(flag uint8) string {
	switch flag {
	case 0:
		return "mobile"
	case 1:
		return "web"
	case 2:
		return "desktop"
	case 99:
		return "system"
	default:
		return "unknown"
	}
}

func (a *App) ownerRoute(cmd ActivateCommand) (OwnerRoute, error) {
	hashSlot := uint16(0)
	if a.hashSlot != nil {
		var err error
		hashSlot, err = a.hashSlot(cmd.UID)
		if err != nil {
			return OwnerRoute{}, err
		}
	}
	ownerSeq := cmd.SessionID
	if a.ownerSeq != nil {
		ownerSeq = a.ownerSeq(cmd.UID)
	}
	return OwnerRoute{
		UID:                    cmd.UID,
		HashSlot:               hashSlot,
		OwnerNodeID:            a.ownerNodeID,
		OwnerBootID:            a.ownerBootID,
		OwnerSeq:               ownerSeq,
		SessionID:              cmd.SessionID,
		DeviceID:               cmd.DeviceID,
		AppInstanceID:          cmd.AppInstanceID,
		DeviceSessionID:        cmd.DeviceSessionID,
		IMSessionID:            cmd.IMSessionID,
		InstallationGeneration: cmd.InstallationGeneration,
		SessionGeneration:      cmd.SessionGeneration,
		AuthorizationFence:     cmd.AuthorizationFence,
		ProtocolVersion:        cmd.ProtocolVersion,
		DeviceFlag:             cmd.DeviceFlag,
		DeviceLevel:            cmd.DeviceLevel,
		Listener:               cmd.Listener,
		ConnectedUnix:          cmd.ConnectedUnix,
		LastActivityUnix:       cmd.ConnectedUnix,
	}, nil
}

func routeFromOwnerRoute(conn OwnerRoute) Route {
	return Route{
		UID:                    conn.UID,
		OwnerNodeID:            conn.OwnerNodeID,
		OwnerBootID:            conn.OwnerBootID,
		OwnerSeq:               conn.OwnerSeq,
		SessionID:              conn.SessionID,
		DeviceID:               conn.DeviceID,
		AppInstanceID:          conn.AppInstanceID,
		DeviceSessionID:        conn.DeviceSessionID,
		IMSessionID:            conn.IMSessionID,
		InstallationGeneration: conn.InstallationGeneration,
		SessionGeneration:      conn.SessionGeneration,
		AuthorizationFence:     conn.AuthorizationFence,
		ProtocolVersion:        conn.ProtocolVersion,
		DeviceFlag:             conn.DeviceFlag,
		DeviceLevel:            conn.DeviceLevel,
		Listener:               conn.Listener,
		ConnectedUnix:          conn.ConnectedUnix,
		LastSeenUnix:           conn.LastActivityUnix,
	}
}
