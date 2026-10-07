package autoscaler

import "time"

// Clock 抽象时间源，测试中使用 FakeClock 以保证决策可复现。
type Clock interface {
	Now() time.Time
}

// RealClock 真实时钟。
type RealClock struct{}

// Now 返回当前真实时间。
func (RealClock) Now() time.Time { return time.Now() }

// FakeClock 可手动推进的时钟，用于确定性测试。
type FakeClock struct {
	now time.Time
}

// NewFakeClock 创建以 t 为起点的假时钟。
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{now: t} }

// Now 返回假时钟当前时间。
func (c *FakeClock) Now() time.Time { return c.now }

// Advance 推进假时钟。
func (c *FakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }
