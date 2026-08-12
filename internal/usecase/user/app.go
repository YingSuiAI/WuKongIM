package user

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/contracts/protocolmeta"
	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
)

// UpdateToken creates missing user metadata and upserts the selected device token.
func (a *App) UpdateToken(ctx context.Context, cmd UpdateTokenCommand) error {
	if err := cmd.validate(); err != nil {
		return err
	}
	if a != nil && a.systemUID != "" && cmd.UID == a.systemUID {
		return errors.New("系统账号不允许更新token！")
	}
	if a == nil || a.users == nil {
		return ErrUserStoreRequired
	}
	if a.devices == nil {
		return ErrDeviceStoreRequired
	}
	if _, err := a.users.GetUser(ctx, cmd.UID); errors.Is(err, metadb.ErrNotFound) {
		if createErr := a.users.CreateUser(ctx, metadb.User{UID: cmd.UID}); createErr != nil && !errors.Is(createErr, metadb.ErrAlreadyExists) {
			return createErr
		}
	} else if err != nil {
		return err
	}
	if a.deviceReader != nil {
		existing, err := a.deviceReader.GetDevice(ctx, cmd.UID, int64(cmd.DeviceFlag), cmd.DeviceID, cmd.AppInstanceID)
		if err == nil && deviceCredentialNewer(existing, cmd) {
			return metadb.ErrStaleMeta
		}
		if err != nil && !errors.Is(err, metadb.ErrNotFound) {
			return err
		}
	}
	if err := a.devices.UpsertDevice(ctx, metadb.Device{
		UID:                    cmd.UID,
		DeviceFlag:             int64(cmd.DeviceFlag),
		DeviceID:               cmd.DeviceID,
		AppInstanceID:          cmd.AppInstanceID,
		DeviceSessionID:        cmd.DeviceSessionID,
		IMSessionID:            cmd.IMSessionID,
		InstallationGeneration: cmd.InstallationGeneration,
		SessionGeneration:      cmd.SessionGeneration,
		AuthorizationFence:     cmd.AuthorizationFence,
		Token:                  cmd.Token,
		DeviceLevel:            int64(cmd.DeviceLevel),
	}); err != nil {
		return err
	}
	if cmd.DeviceLevel == protocolmeta.DeviceLevelMaster {
		a.kickLocalInstallation(cmd.UID, cmd.DeviceFlag, cmd.DeviceID, cmd.SessionGeneration, updateTokenCloseDelay, updateTokenKickReason)
	}
	return nil
}

func deviceCredentialNewer(existing metadb.Device, incoming UpdateTokenCommand) bool {
	if existing.InstallationGeneration != incoming.InstallationGeneration {
		return existing.InstallationGeneration > incoming.InstallationGeneration
	}
	if existing.SessionGeneration != incoming.SessionGeneration {
		return existing.SessionGeneration > incoming.SessionGeneration
	}
	return existing.AuthorizationFence > incoming.AuthorizationFence
}

// DeviceQuit clears stored device tokens and closes matching owner-local sessions.
func (a *App) DeviceQuit(ctx context.Context, cmd DeviceQuitCommand) error {
	if a == nil || a.devices == nil || a.deviceReader == nil {
		return ErrDeviceStoreRequired
	}
	if cmd.UID == "" || cmd.DeviceID == "" {
		return metadb.ErrInvalidArgument
	}
	for _, flag := range deviceQuitFlags(cmd.DeviceFlag) {
		if err := a.quitDevice(ctx, cmd.UID, flag, cmd.DeviceID); err != nil {
			return err
		}
	}
	return nil
}

// OnlineStatus returns legacy online-status entries for online devices only.
func (a *App) OnlineStatus(ctx context.Context, uids []string) ([]OnlineStatus, error) {
	if len(uids) == 0 {
		return nil, nil
	}
	if a != nil && a.presence != nil {
		routesByUID, err := a.presence.EndpointsByUIDs(ctx, uids)
		if err != nil {
			return nil, err
		}
		statuses := make([]OnlineStatus, 0)
		for _, uid := range uids {
			for _, route := range routesByUID[uid] {
				statuses = append(statuses, OnlineStatus{
					UID:        route.UID,
					DeviceFlag: route.DeviceFlag,
					Online:     1,
				})
			}
		}
		return statuses, nil
	}
	return nil, nil
}

// AddSystemUIDs persists system account UIDs and adds them to the local cache.
func (a *App) AddSystemUIDs(ctx context.Context, uids []string) error {
	if len(uids) == 0 {
		return nil
	}
	if a == nil || a.systemUIDs == nil {
		return ErrUserStoreRequired
	}
	if err := a.systemUIDs.AddChannelSubscribers(ctx, systemUIDChannelID, systemUIDChannelType, uids); err != nil {
		return err
	}
	return a.AddSystemUIDsToCache(uids)
}

// RemoveSystemUIDs removes persisted system account UIDs and local cache rows.
func (a *App) RemoveSystemUIDs(ctx context.Context, uids []string) error {
	if len(uids) == 0 {
		return nil
	}
	if a == nil || a.systemUIDs == nil {
		return ErrUserStoreRequired
	}
	if err := a.systemUIDs.RemoveChannelSubscribers(ctx, systemUIDChannelID, systemUIDChannelType, uids); err != nil {
		return err
	}
	return a.RemoveSystemUIDsFromCache(uids)
}

// ListSystemUIDs returns the persisted system account UID list.
func (a *App) ListSystemUIDs(ctx context.Context) ([]string, error) {
	return a.listSystemUIDs(ctx, false)
}

func (a *App) listSystemUIDs(
	ctx context.Context,
	forRestore bool,
) ([]string, error) {
	if a == nil || a.systemUIDs == nil {
		return nil, ErrUserStoreRequired
	}
	list := a.systemUIDs.ListChannelSubscribers
	if forRestore {
		if restoreStore, ok := a.systemUIDs.(RestoreSystemUIDStore); ok {
			list = restoreStore.ListChannelSubscribersForRestore
		}
	}
	var out []string
	cursor := ""
	for {
		uids, nextCursor, done, err := list(
			ctx, systemUIDChannelID, systemUIDChannelType,
			cursor, systemUIDPageLimit,
		)
		if err != nil {
			return nil, err
		}
		out = append(out, uids...)
		if done || nextCursor == "" || nextCursor == cursor {
			return out, nil
		}
		cursor = nextCursor
	}
}

// ReloadSystemUIDCache replaces the privilege cache from the restored durable
// subscriber set before foreground admission resumes.
func (a *App) ReloadSystemUIDCache(ctx context.Context) error {
	uids, err := a.listSystemUIDs(ctx, true)
	if err != nil {
		return err
	}
	next := make(map[string]struct{}, len(uids))
	for _, uid := range uids {
		if uid != "" {
			next[uid] = struct{}{}
		}
	}
	a.systemUIDCacheMu.Lock()
	a.systemUIDCache = next
	a.systemUIDCacheMu.Unlock()
	return nil
}

// AddSystemUIDsToCache adds UIDs to the process-local system account cache.
func (a *App) AddSystemUIDsToCache(uids []string) error {
	if a == nil {
		return nil
	}
	a.systemUIDCacheMu.Lock()
	defer a.systemUIDCacheMu.Unlock()
	if a.systemUIDCache == nil {
		a.systemUIDCache = make(map[string]struct{})
	}
	for _, uid := range uids {
		if uid == "" {
			continue
		}
		a.systemUIDCache[uid] = struct{}{}
	}
	return nil
}

// RemoveSystemUIDsFromCache removes UIDs from the process-local system account cache.
func (a *App) RemoveSystemUIDsFromCache(uids []string) error {
	if a == nil {
		return nil
	}
	a.systemUIDCacheMu.Lock()
	defer a.systemUIDCacheMu.Unlock()
	for _, uid := range uids {
		delete(a.systemUIDCache, uid)
	}
	return nil
}

// IsSystemUID reports whether uid is currently in the process-local system cache.
func (a *App) IsSystemUID(uid string) bool {
	if a == nil {
		return false
	}
	if uid != "" && uid == a.systemUID {
		return true
	}
	a.systemUIDCacheMu.RLock()
	defer a.systemUIDCacheMu.RUnlock()
	_, ok := a.systemUIDCache[uid]
	return ok
}

func (a *App) quitDevice(ctx context.Context, uid string, flag protocolmeta.DeviceFlag, deviceID string) error {
	reader, ok := a.deviceReader.(interface {
		ListDevicesByUID(context.Context, string) ([]metadb.Device, error)
	})
	if !ok {
		return ErrDeviceStoreRequired
	}
	devices, err := reader.ListDevicesByUID(ctx, uid)
	if err != nil {
		return err
	}
	for _, device := range devices {
		if device.DeviceFlag != int64(flag) || device.DeviceID != deviceID {
			continue
		}
		device.Token = deviceQuitMissingToken
		device.DeviceLevel = int64(protocolmeta.DeviceLevelMaster)
		if err := a.devices.UpsertDevice(ctx, device); err != nil {
			return err
		}
		a.kickLocalInstallation(uid, flag, deviceID, device.SessionGeneration, deviceQuitCloseDelay, "")
	}
	return nil
}

func (a *App) kickLocalInstallation(uid string, flag protocolmeta.DeviceFlag, deviceID string, throughGeneration uint64, delay time.Duration, reason string) {
	if a == nil || a.online == nil || a.afterFunc == nil {
		return
	}
	for _, session := range a.online.LocalSessionsByUID(uid) {
		if session.Route.DeviceFlag != uint8(flag) || session.Route.DeviceID != deviceID || session.Route.SessionGeneration > throughGeneration || session.Session == nil {
			continue
		}
		sessionID := session.Route.SessionID
		handle := session.Session
		a.afterFunc(delay, func() {
			_ = handle.CloseSession(reason)
			a.online.MarkClosingAndUnregister(sessionID)
		})
	}
}

func deviceQuitFlags(flag int) []protocolmeta.DeviceFlag {
	if flag == -1 {
		return []protocolmeta.DeviceFlag{protocolmeta.DeviceFlagApp, protocolmeta.DeviceFlagWeb, protocolmeta.DeviceFlagPC}
	}
	return []protocolmeta.DeviceFlag{protocolmeta.DeviceFlag(flag)}
}
