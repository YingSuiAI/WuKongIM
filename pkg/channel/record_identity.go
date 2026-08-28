package channel

import "bytes"

// SameServerAllocatedLogicalRecords reports whether two record lists represent
// the same ordered logical SEND while allowing server-owned durable identity to
// differ. ID, Index, Epoch, ServerTimestampMS, and SizeBytes are assigned or
// derived by the server; every client-observable message-semantic field is
// compared. Empty sender or client keys cannot establish cross-client identity.
func SameServerAllocatedLogicalRecords(left, right []Record) bool {
	if len(left) == 0 || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].FromUID == "" || left[index].ClientMsgNo == "" ||
			left[index].FromUID != right[index].FromUID ||
			left[index].ClientMsgNo != right[index].ClientMsgNo ||
			left[index].Setting != right[index].Setting ||
			left[index].SyncOnce != right[index].SyncOnce ||
			!bytes.Equal(left[index].Payload, right[index].Payload) {
			return false
		}
	}
	return true
}
