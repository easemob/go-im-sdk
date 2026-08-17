package sdk

import (
	"sync"
	"time"
)

// traceIDGenerator 生成用于请求追踪的唯一 trace id。
//
// 位分配与 emclient-linux 的 MsyncTraceIDGenerator 完全一致：
//   - 42 位：毫秒级时间戳（Unix epoch 起，约支持 139 年）
//   - 22 位：同一毫秒内的序列号（约 400 万/毫秒）
//
// 线程安全；处理时钟回拨（不回退、不产生重复）与同毫秒序列号溢出。
type traceIDGenerator struct {
	mu       sync.Mutex
	lastMs   uint64 // 上次使用的毫秒时间戳
	sequence uint64 // 当前毫秒内的序列号
}

const (
	traceTimestampBits = 42
	traceSequenceBits  = 22
	traceSequenceMask  = (uint64(1) << traceSequenceBits) - 1 // 2^22 - 1
)

// next 返回下一个唯一 trace id。
//
// 算法与 C++ 的 MsyncTraceIDGenerator::generateID() 一致，但修正了其
// 时钟回拨处理：当系统时间回拨时沿用 lastMs 并递增序列号，保证 id 不回退、
// 不重复；同毫秒内序列号耗尽时短暂等待时钟前进到下一毫秒。
func (g *traceIDGenerator) next() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	ms := uint64(time.Now().UnixMilli())
	if ms > g.lastMs {
		// 新的毫秒：重置序列号。
		g.lastMs = ms
		g.sequence = 0
	} else {
		// 同毫秒或时钟回拨：沿用 lastMs（不回退），递增序列号。
		g.sequence = (g.sequence + 1) & traceSequenceMask
		if g.sequence == 0 {
			// 序列号在 2^22 内耗尽：等待时钟前进到新毫秒。
			for ms <= g.lastMs {
				time.Sleep(100 * time.Microsecond)
				ms = uint64(time.Now().UnixMilli())
			}
			g.lastMs = ms
		}
	}
	return (g.lastMs << traceSequenceBits) | g.sequence
}

// nextTraceID 返回下一个请求追踪 ID（包级单例，供 REST 与日志使用）。
func nextTraceID() uint64 { return defaultTraceIDGen.next() }

var defaultTraceIDGen traceIDGenerator
