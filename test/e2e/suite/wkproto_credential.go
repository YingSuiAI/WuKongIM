//go:build e2e

package suite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

type wkProtoEndpoint struct {
	apiAddr      string
	serviceToken string
	nodeID       uint64
}

type wkProtoCredential struct {
	appInstanceID          string
	token                  string
	installationGeneration uint64
	sessionGeneration      uint64
	err                    error
}

var wkProtoEndpoints sync.Map

func registerWKProtoEndpoint(spec NodeSpec) {
	serviceToken := defaultE2EServiceToken
	if configured := strings.TrimSpace(spec.ConfigOverrides["WK_API_SERVICE_TOKEN"]); configured != "" {
		serviceToken = configured
	}
	wkProtoEndpoints.Store(spec.GatewayAddr, wkProtoEndpoint{apiAddr: spec.APIAddr, serviceToken: serviceToken, nodeID: spec.ID})
}

func wkProtoCredentialForEndpoint(ctx context.Context, gatewayAddr, uid, deviceID string) (wkProtoCredential, bool) {
	raw, ok := wkProtoEndpoints.Load(gatewayAddr)
	if !ok {
		return wkProtoCredential{}, false
	}
	endpoint := raw.(wkProtoEndpoint)
	credential := wkProtoCredential{
		appInstanceID:          deviceID + "-app",
		token:                  deviceID + "-token",
		installationGeneration: 1,
		sessionGeneration:      1,
	}
	credential.err = provisionWKProtoCredential(ctx, endpoint, uid, deviceID, credential)
	return credential, true
}

func provisionWKProtoCredential(ctx context.Context, endpoint wkProtoEndpoint, uid, deviceID string, credential wkProtoCredential) error {
	body, err := json.Marshal(map[string]any{
		"uid": uid, "device_id": deviceID, "app_instance_id": credential.appInstanceID,
		"device_session_id": deviceID + "-session", "im_session_id": deviceID + "-im",
		"installation_generation": credential.installationGeneration,
		"session_generation":      credential.sessionGeneration, "authorization_fence": 1,
		"token": credential.token, "device_flag": 0, "device_level": 1,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+endpoint.apiAddr+"/user/token", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+endpoint.serviceToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("provision wkproto credential on node %d returned %d: %s", endpoint.nodeID, resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return nil
}
