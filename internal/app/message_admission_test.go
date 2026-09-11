package app

import (
	"context"
	"testing"

	accessapi "github.com/WuKongIM/WuKongIM/internal/access/api"
	"github.com/WuKongIM/WuKongIM/internal/infra/messageadmission"
	"github.com/stretchr/testify/require"
)

func TestApplicationAdmissionWiringRequiresAuthorityAndKeepsPluginsIndependent(t *testing.T) {
	config := Config{Message: MessageConfig{AdmissionURL: "http://platform.internal/internal/im/message-admissions"}}
	_, err := newTestApp(t, config, WithCluster(&fakeCluster{}), WithGateway(nil))
	require.ErrorIs(t, err, messageadmission.ErrInvalidConfiguration)
	config.Webhook.SigningSecret = "test-secret"
	config.API.ListenAddr = "127.0.0.1:0"
	_, err = newTestApp(t, config, WithCluster(&fakeCluster{}), WithGateway(nil))
	require.ErrorIs(t, err, ErrInvalidConfig)
	config.API.ServiceToken = "service-secret"
	app, err := newTestApp(t, config, WithCluster(&fakeCluster{}), WithGateway(nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, app.Stop(context.Background())) })
	require.NotNil(t, app.messageAdmission)
	require.NotNil(t, app.applicationMessageIDReader())
	require.NotNil(t, app.api.(*accessapi.Server))
	require.False(t, app.cfg.Plugin.Enable)
}
