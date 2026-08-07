// Command migrate applies the ATOS Postgres schema. Run it once before
// starting cmd/api with ATOS_DATABASE_URL set.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tosnetwork/atos/migrations"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	databaseURL := os.Getenv("ATOS_DATABASE_URL")
	if databaseURL == "" {
		logger.Error("ATOS_DATABASE_URL is required")
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		logger.Error("connect", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := migrations.Apply(ctx, pool); err != nil {
		logger.Error("migrate", "error", err)
		os.Exit(1)
	}
	logger.Info("migrations applied")
}
