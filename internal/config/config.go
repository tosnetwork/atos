// Package config loads ATOS's runtime configuration from the environment.
package config

import "os"

type Config struct {
	Addr string
}

func Load() Config {
	addr := os.Getenv("ATOS_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	return Config{Addr: addr}
}
