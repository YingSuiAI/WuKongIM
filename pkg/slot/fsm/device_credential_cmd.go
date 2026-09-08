package fsm

// applyResult is read again after the physical commit. A rejected credential
// therefore owns only its logical stale result and cannot poison another Slot.
func (c *upsertDeviceCmd) applyResult() []byte {
	if c.result != nil && c.result.Stale {
		return []byte(ApplyResultStaleMeta)
	}
	return []byte(ApplyResultOK)
}

// IsDeviceCredentialCommand reports whether a committed command updates a
// device credential and must return its stale-incarnation no-op to the caller.
func IsDeviceCredentialCommand(data []byte) bool {
	return len(data) >= headerSize && data[0] == commandVersion && data[1] == cmdTypeUpsertDevice
}
