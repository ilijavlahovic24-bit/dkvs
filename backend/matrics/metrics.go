package metrics

import (
	"dkvs/web"
	"net/http"
	"sync/atomic"
)

type Metrics struct {
	Uptime    string
	Reads     uint64
	Writes    uint64
	Errors    uint64
	KeyCount  int
	ShardName string
	ShardIdx  int
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
func MeasureSetHandler(s *web.Server, m *Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.IncrementWrites()
		s.SetHandler(w, r)
	}
}
func MeasureGetHandler(s *web.Server, m *Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.IncrementReads()
		s.GetHandler(w, r)
	}
}
