// Package migrations embeds the SQL migration files in this directory and
// applies them against Postgres in filename order, tracking what has
// already run in a schema_migrations table. This is intentionally minimal
// (no down-migrations, no external migrate dependency) — enough for
// Phase 1's single-direction schema evolution.
package migrations

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

type migration struct {
	name string
	sql  string
}

// All returns every embedded migration sorted by filename, so
// "001_init.sql" always runs before "002_whatever.sql".
func All() ([]migration, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	var out []migration
	for _, name := range names {
		b, err := files.ReadFile(name)
		if err != nil {
			return nil, err
		}
		out = append(out, migration{name: name, sql: string(b)})
	}
	return out, nil
}

// Apply runs every not-yet-applied migration, each in its own transaction,
// recording success in schema_migrations before moving to the next.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("migrations: create tracking table: %w", err)
	}

	set, err := All()
	if err != nil {
		return fmt.Errorf("migrations: read embedded files: %w", err)
	}

	for _, m := range set {
		var applied bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name=$1)`, m.name).Scan(&applied); err != nil {
			return fmt.Errorf("migrations: check %s: %w", m.name, err)
		}
		if applied {
			continue
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("migrations: begin %s: %w", m.name, err)
		}
		if _, err := tx.Exec(ctx, m.sql); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrations: apply %s: %w", m.name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, m.name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrations: record %s: %w", m.name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("migrations: commit %s: %w", m.name, err)
		}
	}
	return nil
}
