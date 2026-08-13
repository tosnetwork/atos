package main

import (
	"context"
	"errors"
	"testing"
)

func TestReadinessGroupFailsClosed(t *testing.T) {
	if err := (readinessGroup{readinessFunc(func(context.Context) error { return nil }), nil}).CheckReady(context.Background()); err == nil {
		t.Fatal("missing dependency passed readiness")
	}
	want := errors.New("database unavailable")
	if err := (readinessGroup{readinessFunc(func(context.Context) error { return want })}).CheckReady(context.Background()); !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
	if err := (readinessGroup{readinessFunc(func(context.Context) error { return nil })}).CheckReady(context.Background()); err != nil {
		t.Fatal(err)
	}
}
