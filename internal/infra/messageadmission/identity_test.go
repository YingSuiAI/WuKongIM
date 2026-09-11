package messageadmission

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvelopeIdentityReaderUsesOnlyBoundedTopLevelIdentity(t *testing.T) {
	reader := EnvelopeIdentityReader{}
	id, err := reader.ReadApplicationMessageID([]byte(`{"id":"019c0000-0000-7000-8000-000000000002","body":{"id":"forged-body-id"}}`))
	require.NoError(t, err)
	require.Equal(t, "019c0000-0000-7000-8000-000000000002", id)
	for _, input := range []string{"", "null", "[]", `{}`, `{"id":1}`, `{"id":""}`, `{"id":" bad"}`, `{"id":"x\ny"}`, `{"id":"` + strings.Repeat("x", 129) + `"}`, `{"id":"019c0000-0000-4000-8000-000000000002"}`} {
		_, err := reader.ReadApplicationMessageID([]byte(input))
		require.ErrorIs(t, err, ErrInvalidIdentity)
	}
}
