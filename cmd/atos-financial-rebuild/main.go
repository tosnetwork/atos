// Command atos-financial-rebuild deterministically rebuilds disposable ATOS
// projections from sealed evidence after independently checking Blnk.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tosnetwork/atos/internal/financial"
)

func main() {
	databaseURL := flag.String("database-url", "", "operator/migration PostgreSQL URL")
	blnkURL := flag.String("blnk-url", "", "Blnk API base URL")
	gateway := flag.String("gateway", "", "expected gateway identity")
	network := flag.String("network", "", "expected network identity")
	flag.Parse()
	if *databaseURL == "" || *blnkURL == "" || *gateway == "" || *network == "" {
		fmt.Fprintln(os.Stderr, "database-url, blnk-url, gateway and network are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, *databaseURL)
	if err != nil {
		fail(err)
	}
	defer pool.Close()
	repository, err := financial.NewRepository(pool, *gateway, *network)
	if err != nil {
		fail(err)
	}
	client, err := financial.NewBlnkClient(financial.BlnkConfig{BaseURL: *blnkURL, Timeout: 30 * time.Second})
	if err != nil {
		fail(err)
	}
	adapter, err := financial.NewAdapter(repository, client)
	if err != nil {
		fail(err)
	}
	if err := adapter.RebuildProjections(ctx); err != nil {
		fail(err)
	}
	fmt.Println("financial projections rebuilt and verified; safe mode was not cleared")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "projection rebuild failed:", err)
	os.Exit(1)
}
