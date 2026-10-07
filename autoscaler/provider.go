package autoscaler

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Provider 抽象底层容量平台（本地模拟或回环实现）。
// 所有方法幂等：相同 ID 的重复调用返回相同结果。
type Provider interface {
	// Create 在指定可用区创建一个实例，返回实例 ID。
	// zone 为空时由平台自选。
	Create(zone string) (string, error)
	// Terminate 回收一个已完成排空的实例。
	Terminate(id string) error
	// ZoneOf 查询实例所在可用区。
	ZoneOf(id string) (string, error)
	// Alive 报告实例是否仍然存活（未失联）。
	Alive(id string) bool
	// Zones 返回全部可用区。
	Zones() []string
}

// SimProvider 本地模拟的容量平台，支持故障注入。
type SimProvider struct {
	mu    sync.Mutex
	clock Clock
	seq   int
	zones []string
	next  int // 轮询游标，保证均匀分布
	lost  map[string]bool
	// FailNextCreates 注入接下来 N 次创建失败。
	FailNextCreates int
	// FailZones 指定可用区创建必失败。
	FailZones map[string]bool
}

// NewSimProvider 创建模拟平台。
func NewSimProvider(clock Clock, zones []string) *SimProvider {
	if len(zones) == 0 {
		zones = []string{"zone-a"}
	}
	return &SimProvider{
		clock:     clock,
		zones:     append([]string(nil), zones...),
		lost:      map[string]bool{},
		FailZones: map[string]bool{},
	}
}

// Create 模拟创建实例。
func (p *SimProvider) Create(zone string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.FailNextCreates > 0 {
		p.FailNextCreates--
		return "", fmt.Errorf("simulated create failure")
	}
	if zone == "" {
		zone = p.zones[p.next%len(p.zones)]
		p.next++
	}
	if p.FailZones[zone] {
		return "", fmt.Errorf("zone %s unavailable", zone)
	}
	p.seq++
	return fmt.Sprintf("i-%s-%04d", zone, p.seq), nil
}

// Terminate 模拟回收实例。
func (p *SimProvider) Terminate(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lost[id] {
		return fmt.Errorf("instance %s lost", id)
	}
	return nil
}

// ZoneOf 从实例 ID 解析可用区（模拟实现把 zone 编码进 ID）。
func (p *SimProvider) ZoneOf(id string) (string, error) {
	for _, z := range p.zones {
		if strings.Contains(id, "-"+z+"-") {
			return z, nil
		}
	}
	return "", fmt.Errorf("unknown instance %s", id)
}

// Alive 报告实例是否存活。
func (p *SimProvider) Alive(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.lost[id]
}

// Zones 返回全部可用区（排序保证确定性）。
func (p *SimProvider) Zones() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := append([]string(nil), p.zones...)
	sort.Strings(out)
	return out
}

// LoseNode 注入节点失联故障。
func (p *SimProvider) LoseNode(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lost[id] = true
}
