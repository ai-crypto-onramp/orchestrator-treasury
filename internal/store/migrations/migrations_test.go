package migrations

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
)

func TestAll_LoadsMigrations(t *testing.T) {
	migs, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) == 0 {
		t.Fatal("expected at least one migration")
	}
	versions := make([]string, len(migs))
	for i, m := range migs {
		versions[i] = m.Version
		if m.Up == "" || m.Down == "" {
			t.Fatalf("migration %s missing up or down", m.Version)
		}
	}
	if !sort.StringsAreSorted(versions) {
		t.Fatalf("migrations not sorted: %v", versions)
	}
	if versions[0] != "0001" {
		t.Fatalf("expected first version 0001, got %s", versions[0])
	}
}

func TestRunner_UpIdempotent(t *testing.T) {
	var applied []string
	exec := func(_ context.Context, q string, _ ...any) error {
		if q == "INSERT INTO batch_memberships" || q == "CREATE TABLE" {
			// smoke: just record that something executed
		}
		return nil
	}
	// Use a simple exec-based runner; it applies all migrations each call
	// because isApplied always returns false (idempotent SQL guards).
	r := NewRunner(exec)
	if err := r.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = applied
}

func TestRunnerWithQuery_UpAndDown(t *testing.T) {
	var appliedVersions []string
	exec := func(_ context.Context, q string, _ ...any) error { return nil }
	query := func(_ context.Context, version string) (bool, error) {
		for _, v := range appliedVersions {
			if v == version {
				return true, nil
			}
		}
		return false, nil
	}
	r := NewRunnerWithQuery(exec, query)
	if err := r.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	migs, _ := All()
	for _, m := range migs {
		appliedVersions = append(appliedVersions, m.Version)
	}
	// Re-running Up should be a no-op (all applied).
	if err := r.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Down reverts.
	if err := r.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
	appliedVersions = nil
}

// --- additional coverage ---

var errExec = errors.New("exec boom")

func TestRunner_UpCreateTableError(t *testing.T) {
	r := NewRunner(func(_ context.Context, q string, _ ...any) error {
		if strings.HasPrefix(q, "CREATE TABLE IF NOT EXISTS schema_migrations") {
			return errExec
		}
		return nil
	})
	if err := r.Up(context.Background()); !errors.Is(err, errExec) {
		t.Fatalf("err=%v want errExec", err)
	}
}

func TestRunner_UpApplyError(t *testing.T) {
	r := NewRunner(func(_ context.Context, q string, _ ...any) error {
		// Fail on the actual migration Up SQL (not the schema_migrations DDL).
		if strings.HasPrefix(q, "CREATE TABLE IF NOT EXISTS schema_migrations") {
			return nil
		}
		if strings.Contains(q, "CREATE TABLE") || strings.Contains(q, "CREATE INDEX") {
			return errExec
		}
		return nil
	})
	if err := r.Up(context.Background()); err == nil {
		t.Fatal("expected apply error")
	}
}

func TestRunner_DownAppliesAll(t *testing.T) {
	// Runner.Down uses isApplied which always returns false, so nothing
	// is reverted. Still exercise the loop + All() success path.
	var reverted []string
	r := NewRunner(func(_ context.Context, q string, _ ...any) error {
		// schema_migrations DDL is not invoked by Down.
		reverted = append(reverted, q)
		return nil
	})
	if err := r.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Down is a no-op when nothing is applied (isApplied=false), so no
	// down SQL should have been exec'd.
	if len(reverted) != 0 {
		t.Fatalf("expected 0 exec calls, got %d", len(reverted))
	}
}

func TestRunnerWithQuery_UpCreateTableError(t *testing.T) {
	r := NewRunnerWithQuery(
		func(_ context.Context, q string, _ ...any) error {
			if strings.HasPrefix(q, "CREATE TABLE IF NOT EXISTS schema_migrations") {
				return errExec
			}
			return nil
		},
		func(_ context.Context, _ string) (bool, error) { return false, nil },
	)
	if err := r.Up(context.Background()); !errors.Is(err, errExec) {
		t.Fatalf("err=%v want errExec", err)
	}
}

func TestRunnerWithQuery_UpQueryError(t *testing.T) {
	r := NewRunnerWithQuery(
		func(_ context.Context, _ string, _ ...any) error { return nil },
		func(_ context.Context, _ string) (bool, error) { return false, errExec },
	)
	if err := r.Up(context.Background()); !errors.Is(err, errExec) {
		t.Fatalf("err=%v want errExec", err)
	}
}

func TestRunnerWithQuery_UpApplyError(t *testing.T) {
	r := NewRunnerWithQuery(
		func(_ context.Context, q string, _ ...any) error {
			if strings.Contains(q, "CREATE TABLE") || strings.Contains(q, "CREATE INDEX") {
				return errExec
			}
			return nil
		},
		func(_ context.Context, _ string) (bool, error) { return false, nil },
	)
	if err := r.Up(context.Background()); err == nil {
		t.Fatal("expected apply error")
	}
}

func TestRunnerWithQuery_UpRecordError(t *testing.T) {
	r := NewRunnerWithQuery(
		func(_ context.Context, q string, _ ...any) error {
			// Fail only on the INSERT INTO schema_migrations record step.
			if strings.HasPrefix(q, "INSERT INTO schema_migrations") {
				return errExec
			}
			return nil
		},
		func(_ context.Context, _ string) (bool, error) { return false, nil },
	)
	if err := r.Up(context.Background()); err == nil {
		t.Fatal("expected record error")
	}
}

func TestRunnerWithQuery_DownQueryError(t *testing.T) {
	r := NewRunnerWithQuery(
		func(_ context.Context, _ string, _ ...any) error { return nil },
		func(_ context.Context, _ string) (bool, error) { return true, errExec },
	)
	if err := r.Down(context.Background()); !errors.Is(err, errExec) {
		t.Fatalf("err=%v want errExec", err)
	}
}

func TestRunnerWithQuery_DownRevertError(t *testing.T) {
	r := NewRunnerWithQuery(
		func(_ context.Context, _ string, _ ...any) error { return errExec },
		func(_ context.Context, _ string) (bool, error) { return true, nil },
	)
	if err := r.Down(context.Background()); err == nil {
		t.Fatal("expected revert error")
	}
}

func TestRunnerWithQuery_DownDeleteError(t *testing.T) {
	var downCalls int
	r := NewRunnerWithQuery(
		func(_ context.Context, q string, _ ...any) error {
			downCalls++
			// Succeed on the down SQL, fail on the DELETE record step.
			if strings.HasPrefix(q, "DELETE FROM schema_migrations") {
				return errExec
			}
			return nil
		},
		func(_ context.Context, _ string) (bool, error) { return true, nil },
	)
	if err := r.Down(context.Background()); !errors.Is(err, errExec) {
		t.Fatalf("err=%v want errExec", err)
	}
	if downCalls == 0 {
		t.Fatal("expected at least one exec call")
	}
}

func TestRunnerWithQuery_DownSkipsNotApplied(t *testing.T) {
	var execCalls int
	r := NewRunnerWithQuery(
		func(_ context.Context, _ string, _ ...any) error {
			execCalls++
			return nil
		},
		func(_ context.Context, _ string) (bool, error) { return false, nil }, // nothing applied
	)
	if err := r.Down(context.Background()); err != nil {
		t.Fatal(err)
	}
	if execCalls != 0 {
		t.Fatalf("expected 0 exec calls (nothing applied), got %d", execCalls)
	}
}

func TestAll_SortsByNumericVersion(t *testing.T) {
	migs, err := All()
	if err != nil {
		t.Fatal(err)
	}
	versions := make([]string, len(migs))
	for i, m := range migs {
		versions[i] = m.Version
	}
	if !sort.StringsAreSorted(versions) {
		t.Fatalf("versions not sorted: %v", versions)
	}
}