package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type readinessStub struct{ err error }

func (s readinessStub) CheckReady(context.Context) error { return s.err }

func TestReadyzFailsClosedAndSeparatesLiveness(t *testing.T) {
	tests := []struct {
		name       string
		readiness  ReadinessChecker
		wantStatus int
	}{
		{name: "missing dependency", wantStatus: http.StatusServiceUnavailable},
		{name: "dependency unavailable", readiness: readinessStub{err: errors.New("down")}, wantStatus: http.StatusServiceUnavailable},
		{name: "ready", readiness: readinessStub{}, wantStatus: http.StatusOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{Readiness: test.readiness}
			ready := httptest.NewRecorder()
			server.Mux().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if ready.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d", ready.Code, test.wantStatus)
			}
			live := httptest.NewRecorder()
			server.Mux().ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/livez", nil))
			if live.Code != http.StatusOK {
				t.Fatalf("liveness status=%d", live.Code)
			}
		})
	}
}
