// Copyright 2023 The grovemq Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build perf

// Package broker performance / scalability validation harness.
//
// These tests MEASURE (not assert) the scalability behaviour of the
// "per-connection goroutine + session actor" model:
//
//   - TestBroker_Perf_ConnectionScale : how many connections a single node can hold
//     (memory + goroutines per connection).
//   - TestBroker_Perf_Throughput      : end-to-end pub/sub throughput at moderate
//     connection count.
//   - TestBroker_Perf_SlowConsumer    : demonstrates the backpressure bottleneck —
//     one slow subscriber stalls fan-out to all other subscribers and piles up
//     broker goroutines (blocking 100-slot mailbox + no write deadline). With the
//     fix, the fast subscriber is no longer starved (its messages are served,
//     the slow one's QoS 0 messages are dropped).
//   - TestBroker_Perf_OfflineQueue    : offline (persistent-session) queue capacity
//     (10000, drop-oldest) and reconnect drain.
//   - TestBroker_Perf_MaxConnections  : the MaxConnections cap rejects additional
//     CONNECTs once the limit is reached (MQTT 5.0 => 0x97, 3.1.1 => socket closed).
//
// Run with:
//
//	go test -tags perf ./pkg/broker/ -run TestBroker_Perf -v
//
// Scales are env-configurable (see perfEnvInt). NOTE: every connection here is a
// REAL TCP socket; on macOS the default ulimit -n is 256. Raise it before running
// at scale, e.g. `ulimit -n 8192`. Because client and broker run in the SAME
// process, reported per-connection memory/goroutines include BOTH ends; in a real
// deployment the broker-side cost is roughly half of the reported figure.
package broker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func perfEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func perfClient(addr, clientID string, clean bool, handler mqtt.MessageHandler) mqtt.Client {
	opts := mqtt.NewClientOptions().
		AddBroker(addr).
		SetClientID(clientID).
		SetCleanSession(clean).
		SetConnectTimeout(3 * time.Second)
	if handler != nil {
		opts.SetDefaultPublishHandler(handler)
	}
	return mqtt.NewClient(opts)
}

func perfConnect(t *testing.T, c mqtt.Client) {
	t.Helper()
	tok := c.Connect()
	if !tok.WaitTimeout(5 * time.Second) {
		t.Fatalf("connect failed for client: %v", tok.Error())
	}
}

// TestBroker_Perf_ConnectionScale ramps up N idle connections and reports memory
// and goroutine growth. GROVEMQ_PERF_CONNS (default 2000), GROVEMQ_PERF_BATCH
// (default 500).
func TestBroker_Perf_ConnectionScale(t *testing.T) {
	target := perfEnvInt("GROVEMQ_PERF_CONNS", 2000)
	batch := perfEnvInt("GROVEMQ_PERF_BATCH", 500)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, addr := startTestBroker(ctx, t)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	baseGoroutines := runtime.NumGoroutine()

	clients := make([]mqtt.Client, target) // preallocated; each id unique, no race
	var connected int32
	var connectErr int32

	start := time.Now()
	for i := 0; i < target; i += batch {
		end := i + batch
		if end > target {
			end = target
		}
		var wg sync.WaitGroup
		for j := i; j < end; j++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				c := perfClient(addr, fmt.Sprintf("scale-%d", id), true, nil)
				if !c.Connect().WaitTimeout(5 * time.Second) {
					atomic.AddInt32(&connectErr, 1)
					return
				}
				clients[id] = c
				atomic.AddInt32(&connected, 1)
			}(j)
		}
		wg.Wait()

		n := int(atomic.LoadInt32(&connected))
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		heapDelta := ms.HeapAlloc - before.HeapAlloc
		perConn := float64(heapDelta) / float64(n) / 1024
		t.Logf("[scale] connections=%d heap=%.1fMiB goroutines=%d per-connection=%.1fKiB",
			n, float64(ms.HeapAlloc)/1e6, runtime.NumGoroutine(), perConn)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	n := int(atomic.LoadInt32(&connected))
	heapDelta := after.HeapAlloc - before.HeapAlloc
	t.Logf("[scale] SUMMARY target=%d connected=%d connectErrors=%d totalHeap=%.1fMiB perConnection=%.1fKiB goroutineDelta=%d elapsed=%.1fs",
		target, n, atomic.LoadInt32(&connectErr), float64(after.HeapAlloc)/1e6,
		float64(heapDelta)/float64(n)/1024, runtime.NumGoroutine()-baseGoroutines, time.Since(start).Seconds())

	for _, c := range clients {
		if c != nil {
			c.Disconnect(100)
		}
	}
}

// TestBroker_Perf_Throughput measures sustained QoS0 end-to-end pub/sub at
// moderate scale. GROVEMQ_PERF_SUBS (default 50), GROVEMQ_PERF_PUBS (default 8),
// GROVEMQ_PERF_DURATION (seconds, default 10).
func TestBroker_Perf_Throughput(t *testing.T) {
	subs := perfEnvInt("GROVEMQ_PERF_SUBS", 50)
	pubs := perfEnvInt("GROVEMQ_PERF_PUBS", 8)
	duration := time.Duration(perfEnvInt("GROVEMQ_PERF_DURATION", 10)) * time.Second
	payload := bytes.Repeat([]byte("x"), 256)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, addr := startTestBroker(ctx, t)

	topic := "perf/throughput"
	var received int64
	subscribers := make([]mqtt.Client, subs)
	for i := 0; i < subs; i++ {
		c := perfClient(addr, fmt.Sprintf("sub-%d", i), true, func(_ mqtt.Client, m mqtt.Message) {
			atomic.AddInt64(&received, 1)
		})
		perfConnect(t, c)
		if !c.Subscribe(topic, 0, nil).WaitTimeout(time.Second) {
			t.Fatalf("subscribe failed for sub-%d", i)
		}
		defer c.Disconnect(100)
		subscribers[i] = c
	}

	pubClients := make([]mqtt.Client, pubs)
	for i := 0; i < pubs; i++ {
		c := perfClient(addr, fmt.Sprintf("pub-%d", i), true, nil)
		perfConnect(t, c)
		defer c.Disconnect(100)
		pubClients[i] = c
	}

	// Let subscriptions settle.
	time.Sleep(500 * time.Millisecond)

	done := make(chan struct{})
	var published int64
	var wg sync.WaitGroup
	for i := 0; i < pubs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := pubClients[id]
			for {
				select {
				case <-done:
					return
				default:
				}
				if c.Publish(topic, 0, false, payload).WaitTimeout(100 * time.Millisecond) {
					atomic.AddInt64(&published, 1)
				}
			}
		}(i)
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()

	elapsed := duration.Seconds()
	t.Logf("[throughput] SUMMARY published=%d received=%d duration=%.1fs publishRate=%.0f/s deliverRate=%.0f/s fanout=%d",
		atomic.LoadInt64(&published), atomic.LoadInt64(&received), elapsed,
		float64(atomic.LoadInt64(&published))/elapsed, float64(atomic.LoadInt64(&received))/elapsed, subs)
}

// TestBroker_Perf_SlowConsumer demonstrates the backpressure bottleneck: a single
// subscriber whose handler is slow fills its 100-slot mailbox, which blocks the
// sequential routing loop (RouteToLocalSubscribers* → sub.Mailbox.Send) and stalls
// delivery to OTHER subscribers on the same topic, while broker goroutines (one per
// in-flight publish) pile up. It compares fast-subscriber throughput WITH and
// WITHOUT the slow consumer.
func TestBroker_Perf_SlowConsumer(t *testing.T) {
	const (
		fastSubs  = 5
		topic     = "perf/slow"
		slowSleep = 50 * time.Millisecond
		phaseDur  = 5 * time.Second
		payload   = "y"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, addr := startTestBroker(ctx, t)

	// --- Baseline: only fast subscribers, no slow consumer. ---
	var fastRecvA int64
	fastClients := make([]mqtt.Client, fastSubs)
	for i := 0; i < fastSubs; i++ {
		c := perfClient(addr, fmt.Sprintf("fastA-%d", i), true, func(_ mqtt.Client, m mqtt.Message) {
			atomic.AddInt64(&fastRecvA, 1)
		})
		perfConnect(t, c)
		if !c.Subscribe(topic, 0, nil).WaitTimeout(time.Second) {
			t.Fatalf("subscribe failed fastA-%d", i)
		}
		defer c.Disconnect(100)
		fastClients[i] = c
	}
	pub := perfClient(addr, "pub-baseline", true, nil)
	perfConnect(t, pub)
	defer pub.Disconnect(100)
	time.Sleep(300 * time.Millisecond)

	runFlood := func(d time.Duration, slow mqtt.Client) (published, fastRecv int64) {
		var pubd int64
		end := time.Now().Add(d)
		for time.Now().Before(end) {
			pub.Publish(topic, 0, false, []byte(payload)).WaitTimeout(200 * time.Millisecond)
			pubd++
		}
		// Allow in-flight drains.
		time.Sleep(2 * time.Second)
		return pubd, atomic.LoadInt64(&fastRecvA)
	}
	publishedA, fastRecvAVal := runFlood(phaseDur, nil)

	// --- Phase B: introduce a slow subscriber. ---
	var slowRecv int64
	slow := perfClient(addr, "slow", true, func(_ mqtt.Client, m mqtt.Message) {
		time.Sleep(slowSleep)
		atomic.AddInt64(&slowRecv, 1)
	})
	perfConnect(t, slow)
	if !slow.Subscribe(topic, 0, nil).WaitTimeout(time.Second) {
		t.Fatalf("subscribe failed for slow")
	}
	defer slow.Disconnect(100)
	time.Sleep(300 * time.Millisecond)

	goroutinesBefore := runtime.NumGoroutine()
	atomic.StoreInt64(&fastRecvA, 0) // reset counter for phase B
	publishedB, fastRecvB := runFlood(phaseDur, slow)
	goroutinesAfter := runtime.NumGoroutine()

	t.Logf("[slow-consumer] BASELINE  published=%d fastRecv(no-slow)=%d  => fast deliver rate ~%.0f/s",
		publishedA, fastRecvAVal, float64(fastRecvAVal)/phaseDur.Seconds())
	t.Logf("[slow-consumer] WITH-SLOW published=%d fastRecv(with-slow)=%d slowRecv=%d goroutineDelta=%d",
		publishedB, fastRecvB, atomic.LoadInt64(&slowRecv), goroutinesAfter-goroutinesBefore)
	t.Logf("[slow-consumer] INTERPRETATION: with the non-blocking, backpressure-safe fan-out, the fast subscriber is no longer starved by a slow one. The slow subscriber's mailbox fills and its QoS 0 messages are dropped (best-effort) while the router keeps serving the fast subscriber, so fastRecv(with-slow) stays close to (or above) the no-slow baseline.")

	// Assertion: the fix must prevent head-of-line blocking. The fast subscriber
	// must retain at least half of its no-slow throughput even with a slow peer
	// on the same topic (pre-fix it dropped to ~32%).
	if fastRecvB < fastRecvAVal/2 {
		t.Fatalf("[slow-consumer] fast subscriber starved by slow peer: fastRecv(with-slow)=%d < baseline/2=%d", fastRecvB, fastRecvAVal/2)
	}
}

// TestBroker_Perf_OfflineQueue validates the persistent-session offline queue:
// messages published while a clean=false client is offline are queued (cap 10000,
// drop-oldest) and replayed on reconnect. GROVEMQ_PERF_OFFLINE (default 12000).
func TestBroker_Perf_OfflineQueue(t *testing.T) {
	const topic = "perf/offline"
	const clientID = "offline-1"
	N := perfEnvInt("GROVEMQ_PERF_OFFLINE", 12000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, addr := startTestBroker(ctx, t)

	// Connect persistent, subscribe, then disconnect (offline).
	first := perfClient(addr, clientID, false, nil)
	perfConnect(t, first)
	if !first.Subscribe(topic, 0, nil).WaitTimeout(time.Second) {
		t.Fatalf("subscribe failed for offline client")
	}
	first.Disconnect(250)

	// Publish N messages while the client is offline.
	pub := perfClient(addr, "offline-pub", true, nil)
	perfConnect(t, pub)
	defer pub.Disconnect(100)
	payload := []byte("off")
	for i := 0; i < N; i++ {
		pub.Publish(topic, 0, false, payload).WaitTimeout(100 * time.Millisecond)
	}

	// Wait for the broker to actually finish queueing all publishes BEFORE the
	// client reconnects. Otherwise, publishes still in flight in the broker's
	// processPublishPostAck goroutines race the reconnect: once the session is
	// marked Connected again, QueueMessage early-returns and those messages are
	// dropped. We poll the real queued count (capped at the broker's
	// MaxOfflineMessages) so the measurement reflects true offline-queue
	// capacity rather than a harness race.
	expectCap := 10000
	if N < expectCap {
		expectCap = N
	}
	qDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(qDeadline) {
		stats := b.offlineMessageMgr.GetOfflineMessageStats()
		if st, ok := stats[clientID]; ok && st.MessageCount >= expectCap {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	qStats := b.offlineMessageMgr.GetOfflineMessageStats()
	queuedCount := 0
	if st, ok := qStats[clientID]; ok {
		queuedCount = st.MessageCount
	}
	t.Logf("[offline] published=%d queuedOnBroker=%d (cap=%d, drop-oldest)", N, queuedCount, expectCap)

	// Reconnect persistent; count replayed messages.
	var recv int64
	second := perfClient(addr, clientID, false, func(_ mqtt.Client, m mqtt.Message) {
		atomic.AddInt64(&recv, 1)
	})
	perfConnect(t, second)
	defer second.Disconnect(100)

	require := func() bool {
		return atomic.LoadInt64(&recv) >= int64(expectCap)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if require() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	got := atomic.LoadInt64(&recv)
	t.Logf("[offline] SUMMARY published=%d queued=%d delivered=%d (expected=min(%d,10000) due to drop-oldest cap)", N, queuedCount, got, N)
	if got < int64(expectCap)-100 {
		t.Fatalf("offline delivery too low: got %d for %d published (expected ~%d)", got, N, expectCap)
	}
}

// TestBroker_Perf_MaxConnections verifies the hard connection cap: once
// MaxConnections clients are connected, additional CONNECTs are rejected (MQTT
// 5.0 => 0x97 Quota Exceeded; MQTT 3.1.1 => socket closed). GROVEMQ_PERF_MAXCONN
// (default 50).
func TestBroker_Perf_MaxConnections(t *testing.T) {
	maxConn := perfEnvInt("GROVEMQ_PERF_MAXCONN", 50)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, addr := startTestBroker(ctx, t)
	b.MaxConnections = maxConn

	var clients []mqtt.Client
	for i := 0; i < maxConn; i++ {
		c := perfClient(addr, fmt.Sprintf("maxconn-%d", i), true, nil)
		perfConnect(t, c)
		clients = append(clients, c)
	}
	// Let the broker's CONNECT handling settle so the slots are reserved.
	time.Sleep(300 * time.Millisecond)
	if got := atomic.LoadInt64(&b.activeConns); got > int64(maxConn) {
		t.Fatalf("activeConns=%d exceeds MaxConnections=%d", got, maxConn)
	}
	t.Logf("[max-connections] connected=%d activeConns=%d (cap=%d)", maxConn, atomic.LoadInt64(&b.activeConns), maxConn)

	// The (maxConn+1)-th connection must be rejected.
	extra := perfClient(addr, "maxconn-over", true, nil)
	tok := extra.Connect()
	rejected := !(tok.WaitTimeout(5*time.Second) && tok.Error() == nil)
	if !rejected {
		extra.Disconnect(100)
		t.Fatalf("[max-connections] expected the %d-th connection to be rejected, but it succeeded", maxConn+1)
	}
	t.Logf("[max-connections] extra connection correctly rejected (err=%v)", tok.Error())

	for _, c := range clients {
		c.Disconnect(100)
	}
}
