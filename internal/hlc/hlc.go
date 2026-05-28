// Package hlc implements a Hybrid Logical Clock: timestamps that stay close
// to wall-clock time, capture happens-before causality across nodes, and
// remain strictly monotonic even when the physical clock jumps backward.
// silo stamps every namespace mutation with an HLC so that concurrent
// changes gossiped between nodes get a deterministic total order without a
// coordinator. See Kulkarni et al., "Logical Physical Clocks" (2014).
package hlc

import (
	"fmt"
	"sync"
	"time"
)

// Timestamp is a single HLC reading. Wall is unix-nanosecond physical time,
// Logical disambiguates events sharing a wall instant, and Node is the
// originating node id — used only to break ties so two timestamps from
// different nodes are never equal under Compare, giving a total order.
type Timestamp struct {
	Wall    int64  `json:"wall"`
	Logical uint32 `json:"logical"`
	Node    string `json:"node"`
}

// Compare orders timestamps by (Wall, Logical, Node), returning -1, 0, or 1.
func (t Timestamp) Compare(o Timestamp) int {
	switch {
	case t.Wall != o.Wall:
		if t.Wall < o.Wall {
			return -1
		}
		return 1
	case t.Logical != o.Logical:
		if t.Logical < o.Logical {
			return -1
		}
		return 1
	case t.Node != o.Node:
		if t.Node < o.Node {
			return -1
		}
		return 1
	default:
		return 0
	}
}

// Before reports whether t orders strictly before o.
func (t Timestamp) Before(o Timestamp) bool { return t.Compare(o) < 0 }

// After reports whether t orders strictly after o.
func (t Timestamp) After(o Timestamp) bool { return t.Compare(o) > 0 }

// IsZero reports whether t is the never-set zero value.
func (t Timestamp) IsZero() bool { return t.Wall == 0 && t.Logical == 0 && t.Node == "" }

// String renders a compact, lexically-sortable-per-field form suitable for
// conflict suffixes, e.g. "1700000000000000000.3.node-a".
func (t Timestamp) String() string {
	return fmt.Sprintf("%d.%d.%s", t.Wall, t.Logical, t.Node)
}

// Clock is a node's hybrid logical clock. It is safe for concurrent use;
// every method takes an internal lock.
type Clock struct {
	node string
	now  func() time.Time

	mu      sync.Mutex
	wall    int64
	logical uint32
}

// Option configures a Clock.
type Option func(*Clock)

// WithNow injects the physical clock source. Tests drive time
// deterministically through it; production leaves the default time.Now.
func WithNow(now func() time.Time) Option {
	return func(c *Clock) { c.now = now }
}

// New builds a clock that stamps timestamps with node.
func New(node string, opts ...Option) *Clock {
	c := &Clock{node: node, now: time.Now}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Now returns the timestamp for a local event and advances the clock. The
// result is strictly greater (by Compare) than every timestamp this clock
// has previously returned, even if the physical clock has not advanced or
// has moved backward.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	prevWall := c.wall
	if phys := c.now().UnixNano(); phys > c.wall {
		c.wall = phys
	}
	if c.wall == prevWall {
		c.logical++
	} else {
		c.logical = 0
	}
	return Timestamp{Wall: c.wall, Logical: c.logical, Node: c.node}
}

// Update merges a timestamp received from another node and returns a fresh
// local timestamp that happens-after both the received event and every
// prior local event. Call it on receipt of any HLC-stamped message so this
// node's clock tracks the causal frontier of the cluster.
func (c *Clock) Update(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	prevWall := c.wall
	c.wall = max(c.wall, remote.Wall, c.now().UnixNano())
	switch {
	case c.wall == prevWall && c.wall == remote.Wall:
		if remote.Logical > c.logical {
			c.logical = remote.Logical
		}
		c.logical++
	case c.wall == prevWall:
		c.logical++
	case c.wall == remote.Wall:
		c.logical = remote.Logical + 1
	default:
		c.logical = 0
	}
	return Timestamp{Wall: c.wall, Logical: c.logical, Node: c.node}
}
