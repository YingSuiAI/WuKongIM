package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	metadb "github.com/WuKongIM/WuKongIM/pkg/db/meta"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

func TestVerifyWKProtoToken(t *testing.T) {
	const presentedToken = "presented-token"
	const storedToken = "stored-token"

	tests := []struct {
		name       string
		cluster    ClusterRuntime
		token      string
		wantLevel  frame.DeviceLevel
		wantErr    bool
		wantNoLeak string
	}{
		{
			name:      "matching stored token returns stored device level",
			cluster:   &authMetadataCluster{device: metadb.Device{Token: storedToken, DeviceLevel: 7}},
			token:     storedToken,
			wantLevel: frame.DeviceLevel(7),
		},
		{
			name:       "mismatch fails without leaking token",
			cluster:    &authMetadataCluster{device: metadb.Device{Token: storedToken, DeviceLevel: 1}},
			token:      presentedToken,
			wantErr:    true,
			wantNoLeak: presentedToken,
		},
		{
			name:       "missing device fails closed",
			cluster:    &authMetadataCluster{readErr: metadb.ErrNotFound},
			token:      storedToken,
			wantErr:    true,
			wantNoLeak: storedToken,
		},
		{
			name:       "read error fails closed",
			cluster:    &authMetadataCluster{readErr: errors.New("metadata unavailable")},
			token:      storedToken,
			wantErr:    true,
			wantNoLeak: storedToken,
		},
		{
			name:       "missing metadata store fails closed",
			cluster:    &fakeCluster{},
			token:      storedToken,
			wantErr:    true,
			wantNoLeak: storedToken,
		},
		{
			name:    "empty presented token fails closed",
			cluster: &authMetadataCluster{device: metadb.Device{Token: storedToken, DeviceLevel: 1}},
			token:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &App{cluster: tt.cluster}
			level, err := app.verifyWKProtoToken("uid-1", frame.WEB, tt.token)
			if tt.wantErr {
				if err == nil {
					t.Fatal("verifyWKProtoToken() error = nil, want failure")
				}
				if tt.wantNoLeak != "" && strings.Contains(err.Error(), tt.wantNoLeak) {
					t.Fatalf("verifyWKProtoToken() error leaked token %q: %v", tt.wantNoLeak, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("verifyWKProtoToken() error = %v", err)
			}
			if level != tt.wantLevel {
				t.Fatalf("verifyWKProtoToken() level = %d, want %d", level, tt.wantLevel)
			}
		})
	}
}

type authMetadataCluster struct {
	device  metadb.Device
	readErr error
}

func (*authMetadataCluster) Start(context.Context) error { return nil }

func (*authMetadataCluster) Stop(context.Context) error { return nil }

func (*authMetadataCluster) CreateUserMetadata(context.Context, metadb.User) error { return nil }

func (*authMetadataCluster) GetUserMetadata(context.Context, string) (metadb.User, error) {
	return metadb.User{}, metadb.ErrNotFound
}

func (*authMetadataCluster) UpsertDeviceMetadata(context.Context, metadb.Device) error { return nil }

func (c *authMetadataCluster) GetDeviceMetadata(context.Context, string, int64) (metadb.Device, error) {
	if c.readErr != nil {
		return metadb.Device{}, c.readErr
	}
	return c.device, nil
}

func (c *authMetadataCluster) GetDeviceMetadataAuthoritative(context.Context, string, int64) (metadb.Device, error) {
	if c.readErr != nil {
		return metadb.Device{}, c.readErr
	}
	return c.device, nil
}
