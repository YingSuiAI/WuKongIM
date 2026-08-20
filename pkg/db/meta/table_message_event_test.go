package meta

import "testing"

func TestValidMessageEventAuthoritySequenceAllowsGapsAndRejectsNonIncreasing(t *testing.T) {
	cursor := MessageEventCursor{LastAuthoritySequence: 3}
	tests := []struct {
		name     string
		sequence uint64
		want     bool
	}{
		{name: "later gap", sequence: 8, want: true},
		{name: "same", sequence: 3, want: false},
		{name: "earlier", sequence: 2, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validMessageEventAuthoritySequence(cursor, true, MessageEventAppend{AuthoritySequence: tt.sequence})
			if got != tt.want {
				t.Fatalf("validMessageEventAuthoritySequence(last=3, sequence=%d) = %v, want %v", tt.sequence, got, tt.want)
			}
		})
	}
}
