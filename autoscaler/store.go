package autoscaler

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// PersistentState 控制器需要跨重启保留的全部状态。
type PersistentState struct {
	Instances     map[string]*Instance `json:"instances"`
	Ops           []*OpRecord          `json:"ops"`
	LastScaleUp   time.Time            `json:"lastScaleUp"`
	LastScaleDown time.Time            `json:"lastScaleDown"`
	LastRebalance time.Time            `json:"lastRebalance"`
	OpSeq         int                  `json:"opSeq"`
	Events        []Event              `json:"events"` // 审计事件随状态持久化，重启后仍可追溯
}

// Store 状态持久化接口。
type Store interface {
	Load() (*PersistentState, error)
	Save(s *PersistentState) error
}

// MemStore 内存实现（默认，进程重启即丢失，仅用于纯内存场景）。
type MemStore struct {
	mu    sync.Mutex
	state *PersistentState
}

// NewMemStore 创建内存存储。
func NewMemStore() *MemStore { return &MemStore{} }

// Load 读取状态。
func (m *MemStore) Load() (*PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return nil, nil
	}
	cp := *m.state
	return &cp, nil
}

// Save 写入状态。
func (m *MemStore) Save(s *PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *s
	m.state = &cp
	return nil
}

// FileStore JSON 文件快照实现，重启后可恢复。
// 写入采用 临时文件+rename，保证崩溃时不会留下半写的状态文件。
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore 创建文件存储。
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Load 读取状态；文件不存在时返回 nil。
func (f *FileStore) Load() (*PersistentState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s PersistentState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Save 原子写入状态快照。
func (f *FileStore) Save(s *PersistentState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}
