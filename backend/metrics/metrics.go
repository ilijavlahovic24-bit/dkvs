package metrics

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	StartTime time.Time
	Reads     uint64
	Writes    uint64
	Errors    uint64
	ShardName string
	ShardIdx  int
}

func New(shardName string, shardIdx int) *Metrics {
	return &Metrics{
		StartTime: time.Now(),
		ShardName: shardName,
		ShardIdx:  shardIdx,
	}
}
func (m *Metrics) IncrementReads() {
	atomic.AddUint64(&m.Reads, 1)
}

func (m *Metrics) IncrementWrites() {
	atomic.AddUint64(&m.Writes, 1)
}

func (m *Metrics) IncrementErrors() {
	atomic.AddUint64(&m.Errors, 1)
}
func (m *Metrics) Uptime() string {
	return time.Since(m.StartTime).String()
}
