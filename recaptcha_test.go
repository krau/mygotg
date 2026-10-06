package mygotg

import (
	"fmt"
	"testing"

	"github.com/gotd/td/tgerr"
)

func TestParseRecaptcha(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		action string
		key    string
	}{
		{name: "rpc error", input: fmt.Errorf("send code: %w", tgerr.New(400, "RECAPTCHA_CHECK_login__site-key")).Error(), action: "login", key: "site-key"},
		{name: "underscores in action and key", input: "RECAPTCHA_CHECK_send_code__site_key", action: "send_code", key: "site_key"},
		{name: "leading key underscore", input: "RECAPTCHA_CHECK_login___site-key", action: "login", key: "_site-key"},
		{name: "unrelated error", input: "PHONE_NUMBER_INVALID"},
		{name: "missing payload", input: "RECAPTCHA_CHECK_"},
		{name: "missing separator", input: "RECAPTCHA_CHECK_login_site-key"},
		{name: "empty action", input: "RECAPTCHA_CHECK___site-key"},
		{name: "empty key", input: "RECAPTCHA_CHECK_login__"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			action, key := parseRecaptcha(tc.input)
			if action != tc.action || key != tc.key {
				t.Fatalf("challenge = (%q, %q), want (%q, %q)", action, key, tc.action, tc.key)
			}
		})
	}
}
