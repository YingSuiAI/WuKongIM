package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildConfigReadsMandatoryMessageAdmission(t *testing.T) {
	values := map[string]string{
		"WK_NODE_ID": "1", "WK_NODE_DATA_DIR": t.TempDir(), "WK_CLUSTER_LISTEN_ADDR": "127.0.0.1:0",
		"WK_MESSAGE_ADMISSION_URL":     "http://platform.internal/internal/im/message-admissions",
		"WK_MESSAGE_ADMISSION_TIMEOUT": "1500ms", "WK_WEBHOOK_SIGNING_SECRET": "test-secret",
	}
	config, err := buildConfig(values)
	require.NoError(t, err)
	require.Equal(t, values["WK_MESSAGE_ADMISSION_URL"], config.Message.AdmissionURL)
	require.Equal(t, 1500*time.Millisecond, config.Message.AdmissionTimeout)
	values["WK_MESSAGE_ADMISSION_TIMEOUT"] = "-1s"
	_, err = buildConfig(values)
	require.Error(t, err)
}
