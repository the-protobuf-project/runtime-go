package options

import (
	"testing"

	"github.com/nats-io/nats.go"
)

// helper to apply []nats.Option and return the resulting nats.Options
func apply(opts []nats.Option) nats.Options {
	var no nats.Options
	for _, o := range opts {
		o(&no)
	}
	return no
}

func TestValidate_FillsDefaultsWhenEmpty(t *testing.T) {
	// Ensure no env overrides so defaults from loadDefaultConnectionOptions() are used.
	t.Setenv("NATS_URL", "")
	t.Setenv("NATS_NAME", "")
	t.Setenv("NATS_USERNAME", "")
	t.Setenv("NATS_PASSWORD", "")

	in := NatsClientOptions{} // everything empty

	got, connOpts, err := in.Validate()
	if err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	// Should pick up hardcoded defaults from loadDefaultConnectionOptions()
	if got.URL != "nats://localhost:4222" {
		t.Errorf("URL default mismatch: got %q, want %q", got.URL, "nats://localhost:4222")
	}
	if got.Name != "cutlery-service" {
		t.Errorf("Name default mismatch: got %q, want %q", got.Name, "cutlery-service")
	}
	if got.Auth.Username != "" || got.Auth.Password != "" {
		t.Errorf("Auth should be empty by default, got %+v", got.Auth)
	}
	if got.RegularStreamRetention.MaxAge != 0 || got.RegularStreamRetention.MaxBytes != 0 {
		t.Errorf("RegularStreamRetention should stay unbounded when unset: got %+v", got.RegularStreamRetention)
	}

	no := apply(connOpts)
	if no.Name != "cutlery-service" {
		t.Errorf("nats.Options.Name mismatch: got %q, want %q", no.Name, "cutlery-service")
	}
	if no.User != "" || no.Password != "" {
		t.Errorf("nats.Options auth should be empty, got user=%q", no.User)
	}
}

func TestValidate_RegularStreamRetentionExplicitPreservesPartialZero(t *testing.T) {
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("NATS_NAME", "")
	t.Setenv("NATS_USERNAME", "")
	t.Setenv("NATS_PASSWORD", "")

	wantBytes := int64(256 << 20)
	in := NatsClientOptions{
		RegularStreamRetention: RegularStreamRetention{
			MaxAge:   0,
			MaxBytes: wantBytes,
		},
	}

	got, _, err := in.Validate()
	if err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}
	if got.RegularStreamRetention.MaxAge != 0 || got.RegularStreamRetention.MaxBytes != wantBytes {
		t.Errorf("expected partial retention preserved: got %+v", got.RegularStreamRetention)
	}
}

func TestValidate_UsesEnvDefaults(t *testing.T) {
	// Provide env values that loadDefaultConnectionOptions should pick up.
	t.Setenv("NATS_URL", "nats://env-host:4223")
	t.Setenv("NATS_NAME", "env-service")
	t.Setenv("NATS_USERNAME", "env-user")
	t.Setenv("NATS_PASSWORD", "env-pass")

	in := NatsClientOptions{} // everything empty, will be filled from env via defaults

	got, connOpts, err := in.Validate()
	if err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	if got.URL != "nats://env-host:4223" {
		t.Errorf("URL mismatch: got %q, want %q", got.URL, "nats://env-host:4223")
	}
	if got.Name != "env-service" {
		t.Errorf("Name mismatch: got %q, want %q", got.Name, "env-service")
	}
	if got.Auth.Username != "env-user" || got.Auth.Password != "env-pass" {
		t.Errorf("Auth mismatch: got %+v, want user=env-user pass=env-pass", got.Auth)
	}

	no := apply(connOpts)
	if no.Name != "env-service" {
		t.Errorf("nats.Options.Name mismatch: got %q, want %q", no.Name, "env-service")
	}
	if no.User != "env-user" || no.Password != "env-pass" {
		t.Errorf("nats.Options auth mismatch: got user=%q pass=%q", no.User, no.Password)
	}
}

func TestValidate_ExplicitOverridesBeatDefaults(t *testing.T) {
	// Set env defaults, but provide explicit fields in input.
	t.Setenv("NATS_URL", "nats://env-host:4223")
	t.Setenv("NATS_NAME", "env-service")
	t.Setenv("NATS_USERNAME", "env-user")
	t.Setenv("NATS_PASSWORD", "env-pass")

	in := NatsClientOptions{
		URL:  "nats://explicit:4224",
		Name: "explicit-service",
		Auth: NatsAuthOptions{
			Username: "explicit-user",
			Password: "explicit-pass",
		},
	}

	got, connOpts, err := in.Validate()
	if err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	if got.URL != "nats://explicit:4224" {
		t.Errorf("URL mismatch: got %q, want %q", got.URL, "nats://explicit:4224")
	}
	if got.Name != "explicit-service" {
		t.Errorf("Name mismatch: got %q, want %q", got.Name, "explicit-service")
	}
	if got.Auth.Username != "explicit-user" || got.Auth.Password != "explicit-pass" {
		t.Errorf("Auth mismatch: got %+v, want explicit creds", got.Auth)
	}

	no := apply(connOpts)
	if no.Name != "explicit-service" {
		t.Errorf("nats.Options.Name mismatch: got %q, want %q", no.Name, "explicit-service")
	}
	if no.User != "explicit-user" || no.Password != "explicit-pass" {
		t.Errorf("nats.Options auth mismatch: got user=%q pass=%q", no.User, no.Password)
	}
}

func TestValidate_PartialAuth_NoUserInfoApplied(t *testing.T) {
	// No env to interfere.
	t.Setenv("NATS_URL", "")
	t.Setenv("NATS_NAME", "")
	t.Setenv("NATS_USERNAME", "")
	t.Setenv("NATS_PASSWORD", "")

	in := NatsClientOptions{
		// URL left empty so default kicks in
		Auth: NatsAuthOptions{
			Username: "only-user", // password missing → partial auth
		},
	}

	got, connOpts, err := in.Validate()
	if err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	// URL should have been defaulted
	if got.URL == "" {
		t.Fatalf("URL should be defaulted, got empty")
	}

	no := apply(connOpts)
	// With partial auth, we expect Validate not to set UserInfo on the nats.Options.
	if no.User != "" || no.Password != "" {
		t.Errorf("expected no nats auth to be applied for partial creds, got user=%q", no.User)
	}
}

func TestValidate_NoAuth_OK(t *testing.T) {
	t.Setenv("NATS_URL", "nats://host:4222")
	t.Setenv("NATS_NAME", "svc")
	t.Setenv("NATS_USERNAME", "")
	t.Setenv("NATS_PASSWORD", "")

	in := NatsClientOptions{} // will use env/defaults

	_, connOpts, err := in.Validate()
	if err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	no := apply(connOpts)
	if no.User != "" || no.Password != "" {
		t.Errorf("expected no auth applied, got user=%q", no.User)
	}
}
