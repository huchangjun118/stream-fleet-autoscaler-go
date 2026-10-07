package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/huchangjun118/stream-fleet-autoscaler-go/internal/clock"
)

// FileStore persists State as an atomic JSON snapshot (temp file + rename),
// so a crash mid-write never leaves a torn controller state.
type FileStore struct {
	path string
	clk  clock.Clock
}

// NewFileStore creates a file-backed store at path.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path, clk: clock.NewSystem()}
}

// NewFileStoreWithClock creates a file-backed store with an injected clock.
func NewFileStoreWithClock(path string, clk clock.Clock) *FileStore {
	return &FileStore{path: path, clk: clk}
}

// Load implements Store.
func (f *FileStore) Load(ctx context.Context) (*State, error) {
	b, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}
	st := NewState()
	if err := json.Unmarshal(b, st); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	if st.UpBreaches == nil {
		st.UpBreaches = map[string]int{}
	}
	if st.DownBreaches == nil {
		st.DownBreaches = map[string]int{}
	}
	if st.LastRebalanceTick == nil {
		st.LastRebalanceTick = map[string]int64{}
	}
	return st, nil
}

// Save implements Store.
func (f *FileStore) Save(ctx context.Context, st *State) error {
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	dir := filepath.Dir(f.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("state dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return fmt.Errorf("temp state: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return fmt.Errorf("write state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close state: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return fmt.Errorf("commit state: %w", err)
	}
	return nil
}

// AppendEvent implements Store.
func (f *FileStore) AppendEvent(ctx context.Context, st *State, tick int64, evType, msg string) error {
	appendEvent(st, tick, f.clk.Now(), evType, msg, 10000)
	return nil
}
