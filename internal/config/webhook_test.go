package config

import "testing"

func TestBuildConfigReadsWebhookSigningSecret(t *testing.T) {
	cfg, err := buildConfig(map[string]string{
		"WK_NODE_ID":                "1",
		"WK_NODE_DATA_DIR":          t.TempDir(),
		"WK_CLUSTER_LISTEN_ADDR":    "127.0.0.1:0",
		"WK_WEBHOOK_HTTP_ADDR":      "https://example.invalid/hook",
		"WK_WEBHOOK_SIGNING_SECRET": "secret-value",
	})
	if err != nil {
		t.Fatalf("buildConfig() error = %v", err)
	}
	if cfg.Webhook.SigningSecret != "secret-value" {
		t.Fatalf("Webhook.SigningSecret = %q, want secret-value", cfg.Webhook.SigningSecret)
	}
}
