package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type readinessStub struct{ err error }

func (s readinessStub) CheckReady(context.Context) error { return s.err }

func TestReadinessHandlerFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name    string
		checker readinessChecker
		want    int
	}{
		{name: "missing", checker: nil, want: http.StatusServiceUnavailable},
		{name: "failed", checker: readinessStub{err: errors.New("down")}, want: http.StatusServiceUnavailable},
		{name: "ready", checker: readinessStub{}, want: http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			readinessHandler(test.checker, time.Second)(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d", response.Code, test.want)
			}
		})
	}
}
