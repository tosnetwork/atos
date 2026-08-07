// Package config loads ATOS's runtime configuration from the environment.
package config

import "os"

type Config struct {
	Addr string
	// DatabaseURL selects the storage backend: empty uses the Phase 0
	// in-memory store (zero setup, non-durable); set it to use the
	// Phase 1 Postgres store (internal/store/postgres) instead. Run
	// cmd/migrate against the same URL first.
	DatabaseURL string
}

func Load() Config {
	addr := os.Getenv("ATOS_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	return Config{
		Addr:        addr,
		DatabaseURL: os.Getenv("ATOS_DATABASE_URL"),
	}
}
