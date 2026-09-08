package fsm

// IsDeviceCredentialCommand reports whether a committed command updates a
// device credential and must return its stale-incarnation no-op to the caller.
func IsDeviceCredentialCommand(data []byte) bool {
	return len(data) >= headerSize && data[0] == commandVersion && data[1] == cmdTypeUpsertDevice
}
