package helpers

import "os"

// setEnvOrDefault returns the value of envVar if it is set.
//
// If the variable is unset, it returns an empty string in Development mode.
// In other environments, it returns the first provided default value if one
// is given, or an empty string if no default is provided.
func SetEnvOrDefault(envVar string, defaultValue ...string) string {
	if val := os.Getenv(envVar); val != "" {
		return val
	}
	if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return ""
}
