package helpers

import (
	"os"
	"testing"
)

func TestSetEnvOrDefault_EnvVarSet(t *testing.T) {
	t.Setenv("TEST_KEY", "value123")

	got := SetEnvOrDefault("TEST_KEY", "fallback")
	want := "value123"

	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestSetEnvOrDefault_EnvVarUnset_WithDefault(t *testing.T) {
	_ = os.Unsetenv("TEST_KEY")

	got := SetEnvOrDefault("TEST_KEY", "fallback")
	want := "fallback"

	if got != want {
		t.Fatalf("expected default %q, got %q", want, got)
	}
}

func TestSetEnvOrDefault_EnvVarUnset_NoDefault(t *testing.T) {
	_ = os.Unsetenv("TEST_KEY")

	got := SetEnvOrDefault("TEST_KEY")
	want := ""

	if got != want {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestSetEnvOrDefault_EnvVarSet_IgnoresDefault(t *testing.T) {
	t.Setenv("TEST_KEY", "actual")

	got := SetEnvOrDefault("TEST_KEY", "fallback")
	want := "actual"

	if got != want {
		t.Fatalf("expected env value %q, got %q", want, got)
	}
}
