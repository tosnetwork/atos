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
	// BlobDir is where internal/adapters/storage/local writes uploaded
	// file bytes. See ~/atos-spec/docs/ARTIFACTS.md.
	BlobDir string
	// PublicBaseURL is prefixed onto signed upload/download URLs so a
	// client can actually reach this server from outside the process —
	// it cannot be inferred from Addr alone (":8080" isn't a fetchable
	// host).
	PublicBaseURL string
}

func Load() Config {
	addr := os.Getenv("ATOS_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	blobDir := os.Getenv("ATOS_BLOB_DIR")
	if blobDir == "" {
		blobDir = "./data/blobs"
	}
	publicBaseURL := os.Getenv("ATOS_PUBLIC_BASE_URL")
	if publicBaseURL == "" {
		publicBaseURL = "http://localhost:8080"
	}
	return Config{
		Addr:          addr,
		DatabaseURL:   os.Getenv("ATOS_DATABASE_URL"),
		BlobDir:       blobDir,
		PublicBaseURL: publicBaseURL,
	}
}
