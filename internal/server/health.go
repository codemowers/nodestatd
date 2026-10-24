// Package server serves nodestatd's HTTP endpoints.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// Check reports a problem as an error.
type Check func(context.Context) error

// ServeHealth serves /healthz from live checks and /readyz from ready checks
// until ctx is done.
func ServeHealth(ctx context.Context, addr string, live, ready []Check, log *slog.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("/healthz", checksHandler(live))
	mux.Handle("/readyz", checksHandler(ready))
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("health server: %w", err)
	}
	return nil
}

func checksHandler(checks []Check) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		for _, check := range checks {
			if err := check(ctx); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		fmt.Fprintln(w, "ok")
	})
}
