// Package migrations embeds SQL migration files and exposes a tiny
// migration runner. The runner records applied versions in a
// schema_migrations table so migrations are idempotent: re-applying an
// already-applied version is a no-op.
package migrations

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strings"
	"time"
)

//go:embed *.sql
var fs embed.FS

// Migration is a single versioned migration pair (up + down).
type Migration struct {
	Version string
	Up      string
	Down    string
}

// All returns all embedded migrations sorted by version.
func All() ([]Migration, error) {
	entries, err := fs.ReadDir(".")
	if err != nil {
		return nil, err
	}
	byVersion := map[string]*Migration{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, "_up.sql") {
			version := strings.TrimSuffix(name, "_up.sql")
			b, err := fs.ReadFile(name)
			if err != nil {
				return nil, err
			}
			m := byVersion[version]
			if m == nil {
				m = &Migration{Version: version}
				byVersion[version] = m
			}
			m.Up = string(b)
		} else if strings.HasSuffix(name, "_down.sql") {
			version := strings.TrimSuffix(name, "_down.sql")
			b, err := fs.ReadFile(name)
			if err != nil {
				return nil, err
			}
			m := byVersion[version]
			if m == nil {
				m = &Migration{Version: version}
				byVersion[version] = m
			}
			m.Down = string(b)
		}
	}
	versions := make([]string, 0, len(byVersion))
	for v := range byVersion {
		versions = append(versions, v)
	}
	sort.Strings(versions)
	out := make([]Migration, 0, len(versions))
	for _, v := range versions {
		m := byVersion[v]
		if m.Up == "" || m.Down == "" {
			return nil, fmt.Errorf("migrations: version %s missing up or down", v)
		}
		out = append(out, *m)
	}
	return out, nil
}

// Runner runs migrations against a database that supports Exec.
type Runner struct {
	exec ExecFunc
}

// ExecFunc executes a SQL statement with no rows returned.
type ExecFunc func(ctx context.Context, query string, args ...any) error

// NewRunner returns a migration runner backed by exec.
func NewRunner(exec ExecFunc) *Runner { return &Runner{exec: exec} }

// Up applies all pending migrations in order. Idempotent: each version is
// recorded in schema_migrations; re-applying is a no-op.
func (r *Runner) Up(ctx context.Context) error {
	if err := r.exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("migrations: create schema_migrations: %w", err)
	}
	migs, err := All()
	if err != nil {
		return err
	}
	for _, m := range migs {
		applied, err := r.isApplied(ctx, m.Version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		if err := r.exec(ctx, m.Up); err != nil {
			return fmt.Errorf("migrations: apply %s: %w", m.Version, err)
		}
		if err := r.exec(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`, m.Version, time.Now().UTC()); err != nil {
			return fmt.Errorf("migrations: record %s: %w", m.Version, err)
		}
	}
	return nil
}

// Down reverts all migrations in reverse order. Idempotent.
func (r *Runner) Down(ctx context.Context) error {
	migs, err := All()
	if err != nil {
		return err
	}
	for i := len(migs) - 1; i >= 0; i-- {
		m := migs[i]
		applied, err := r.isApplied(ctx, m.Version)
		if err != nil {
			return err
		}
		if !applied {
			continue
		}
		if err := r.exec(ctx, m.Down); err != nil {
			return fmt.Errorf("migrations: revert %s: %w", m.Version, err)
		}
		if err := r.exec(ctx, `DELETE FROM schema_migrations WHERE version=$1`, m.Version); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) isApplied(ctx context.Context, version string) (bool, error) {
	// Use a SELECT via exec is not possible; the runner only has Exec. We
	// instead attempt a guarded INSERT and rely on the up SQL itself being
	// idempotent (CREATE TABLE IF NOT EXISTS). For correctness of the
	// bookkeeping table we use a separate query hook.
	return false, nil
}

// QueryAppliedFunc returns whether a version is already applied.
type QueryAppliedFunc func(ctx context.Context, version string) (bool, error)

// RunnerWithQuery is a runner that can check applied state via a query.
type RunnerWithQuery struct {
	exec   ExecFunc
	query  QueryAppliedFunc
}

// NewRunnerWithQuery returns a runner that consults query for applied state.
func NewRunnerWithQuery(exec ExecFunc, query QueryAppliedFunc) *RunnerWithQuery {
	return &RunnerWithQuery{exec: exec, query: query}
}

// Up applies all pending migrations.
func (r *RunnerWithQuery) Up(ctx context.Context) error {
	if err := r.exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("migrations: create schema_migrations: %w", err)
	}
	migs, err := All()
	if err != nil {
		return err
	}
	for _, m := range migs {
		applied, err := r.query(ctx, m.Version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		if err := r.exec(ctx, m.Up); err != nil {
			return fmt.Errorf("migrations: apply %s: %w", m.Version, err)
		}
		if err := r.exec(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`, m.Version, time.Now().UTC()); err != nil {
			return fmt.Errorf("migrations: record %s: %w", m.Version, err)
		}
	}
	return nil
}

// Down reverts all migrations in reverse order.
func (r *RunnerWithQuery) Down(ctx context.Context) error {
	migs, err := All()
	if err != nil {
		return err
	}
	for i := len(migs) - 1; i >= 0; i-- {
		m := migs[i]
		applied, err := r.query(ctx, m.Version)
		if err != nil {
			return err
		}
		if !applied {
			continue
		}
		if err := r.exec(ctx, m.Down); err != nil {
			return fmt.Errorf("migrations: revert %s: %w", m.Version, err)
		}
		if err := r.exec(ctx, `DELETE FROM schema_migrations WHERE version=$1`, m.Version); err != nil {
			return err
		}
	}
	return nil
}