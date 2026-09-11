package app

import (
	"fmt"
	"strings"

	"github.com/WuKongIM/WuKongIM/internal/contracts/channelappend"
	"github.com/WuKongIM/WuKongIM/internal/infra/messageadmission"
)

func (a *App) wireMessageAdmission() error {
	if a.cfg.Message.AdmissionURL == "" {
		return nil
	}
	if a.cfg.API.ListenAddr != "" && strings.TrimSpace(a.cfg.API.ServiceToken) == "" {
		return fmt.Errorf("%w: message admission requires a service token for the enabled HTTP SEND entry", ErrInvalidConfig)
	}
	client, err := messageadmission.New(messageadmission.Options{
		URL: a.cfg.Message.AdmissionURL, SigningSecret: a.cfg.Webhook.SigningSecret, Timeout: a.cfg.Message.AdmissionTimeout,
	})
	if err != nil {
		return err
	}
	a.messageAdmission = client
	return nil
}

func (a *App) applicationMessageIDReader() channelappend.ApplicationMessageIDReader {
	if a.messageAdmission == nil {
		return nil
	}
	return messageadmission.EnvelopeIdentityReader{}
}
