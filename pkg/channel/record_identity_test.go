package channel

import "testing"

func TestSameServerAllocatedLogicalRecordsCoversRecordSemantics(t *testing.T) {
	base := Record{
		ID: 101, Index: 7, Epoch: 3, ServerTimestampMS: 1001, SizeBytes: 7,
		Setting: 2, FromUID: "sender", ClientMsgNo: "client-1", SyncOnce: true, Payload: []byte("payload"),
	}
	serverRetry := base
	serverRetry.ID = 202
	serverRetry.Index = 0
	serverRetry.Epoch = 0
	serverRetry.ServerTimestampMS = 2002
	serverRetry.SizeBytes = 999
	if !SameServerAllocatedLogicalRecords([]Record{base}, []Record{serverRetry}) {
		t.Fatal("server-owned identity changes should preserve logical equality")
	}

	for _, testCase := range []struct {
		name   string
		mutate func(*Record)
	}{
		{name: "setting", mutate: func(record *Record) { record.Setting++ }},
		{name: "sender", mutate: func(record *Record) { record.FromUID = "other" }},
		{name: "client key", mutate: func(record *Record) { record.ClientMsgNo = "other" }},
		{name: "sync once", mutate: func(record *Record) { record.SyncOnce = false }},
		{name: "payload", mutate: func(record *Record) { record.Payload = []byte("other") }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			changed := serverRetry
			testCase.mutate(&changed)
			if SameServerAllocatedLogicalRecords([]Record{base}, []Record{changed}) {
				t.Fatal("semantic change reported as the same logical SEND")
			}
		})
	}

	emptySender := base
	emptySender.FromUID = ""
	if SameServerAllocatedLogicalRecords([]Record{emptySender}, []Record{emptySender}) {
		t.Fatal("empty sender established cross-client identity")
	}
	emptyClientKey := base
	emptyClientKey.ClientMsgNo = ""
	if SameServerAllocatedLogicalRecords([]Record{emptyClientKey}, []Record{emptyClientKey}) {
		t.Fatal("empty client key established cross-client identity")
	}
}
