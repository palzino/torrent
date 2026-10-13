package torrent

import (
	"net/netip"
	"testing"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/tracker"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// Every test uses a dead loopback port: connect returns ECONNREFUSED
// immediately, so ordinary outbound attempts fail fast and deterministically.
var retryDeadAddr = netip.MustParseAddrPort("127.0.0.1:1")

func retryTestAddrPort(port uint16) netip.AddrPort {
	return netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)
}

func newRetryTestClient(t *testing.T, mutate func(*ClientConfig)) (*Client, *Torrent) {
	t.Helper()
	cfg := NewDefaultClientConfig()
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisableTrackers = true
	cfg.DisableUTP = true
	if mutate != nil {
		mutate(cfg)
	}
	cl, err := NewClient(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { cl.Close() })
	tor, new := cl.AddTorrentInfoHash(metainfo.Hash{1: 1})
	require.True(t, new)
	return cl, tor
}

func parkedCount(tor *Torrent) int {
	tor.cl.lock()
	defer tor.cl.unlock()
	return tor.numFailedPeers()
}

func eventually(t *testing.T, timeout time.Duration, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A failed ordinary dial must be parked, not discarded, and retried only after
// its deadline. When the failcount budget is exhausted the address is evicted,
// which permits rediscovery again.
func TestFailedDialParksAndRetriesAfterDeadline(t *testing.T) {
	cl, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = 100 * time.Millisecond
		cfg.OutgoingPeerRetryMaxDelay = 400 * time.Millisecond
		cfg.OutgoingPeerRetryMaxFailcount = 3
	})
	n := tor.AddPeers([]PeerInfo{{Addr: retryDeadAddr}})
	require.Equal(t, 1, n)

	// First attempt fails and is parked.
	eventually(t, 2*time.Second, "peer parked after refused dial", func() bool {
		c := parkedCount(tor)
		return c == 1 && tor.peers.Len() == 0
	})
	key := retryDeadAddr.String()
	cl.lock()
	failcount := tor.failedPeers[key].failcount
	nextAt := tor.failedPeers[key].nextAt
	cl.unlock()
	require.Equal(t, 1, failcount)
	require.True(t, time.Until(nextAt) <= 150*time.Millisecond, "first retry should be one base delay out")

	// Backoff doubles per failure until the budget is exhausted.
	for want := 2; want <= 3; want++ {
		eventually(t, 3*time.Second, "failcount increments via due retry", func() bool {
			cl.lock()
			defer cl.unlock()
			e, ok := tor.failedPeers[key]
			return ok && e.failcount == want
		})
	}

	// Fourth failure exceeds MaxFailcount: evicted, rediscovery permitted.
	eventually(t, 3*time.Second, "entry evicted after failcount budget", func() bool {
		return parkedCount(tor) == 0
	})
	n = tor.AddPeers([]PeerInfo{{Addr: retryDeadAddr}})
	require.Equal(t, 1, n, "rediscovery must be permitted after eviction")
}

// Rediscovery of a cooling address must not bypass its deadline or create a
// second record.
func TestCooldownSuppressesRediscovery(t *testing.T) {
	cl, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = 500 * time.Millisecond
	})
	tor.AddPeers([]PeerInfo{{Addr: retryDeadAddr}})
	eventually(t, 2*time.Second, "peer parked", func() bool { return parkedCount(tor) == 1 })

	key := retryDeadAddr.String()
	cl.lock()
	e := tor.failedPeers[key]
	wantFailcount, wantNextAt := e.failcount, e.nextAt
	cl.unlock()

	n := tor.AddPeers([]PeerInfo{{Addr: retryDeadAddr}})
	require.Zero(t, n, "cooldown must suppress rediscovery")
	cl.lock()
	defer cl.unlock()
	require.Zero(t, tor.peers.Len(), "cooling address must not re-enter ready")
	require.Equal(t, wantFailcount, tor.failedPeers[key].failcount)
	require.Equal(t, wantNextAt, tor.failedPeers[key].nextAt)
}

// noLongerHalfOpen fans openNewConns out to every torrent on every half-open
// slot release. A parked address must never be redialed by that fan-out before
// its deadline.
func TestFanOutCannotRedialParkedPeer(t *testing.T) {
	cl, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = time.Hour // no due retries during the test
		cfg.TotalHalfOpenConns = 1
		cfg.DialRateLimiter = rate.NewLimiter(3, 1)
	})
	tor.AddPeers([]PeerInfo{{Addr: retryDeadAddr}})
	eventually(t, 2*time.Second, "peer parked", func() bool { return parkedCount(tor) == 1 })

	// Hold the sole global half-open slot on a second dial. The third peer
	// cannot start until noLongerHalfOpen fans out after that slot is released.
	require.True(t, cl.config.DialRateLimiter.Reserve().OK())
	second := retryTestAddrPort(2)
	third := retryTestAddrPort(3)
	tor.AddPeers([]PeerInfo{{Addr: second}})
	eventually(t, 2*time.Second, "second dial became half-open", func() bool {
		cl.lock()
		defer cl.unlock()
		return tor.connectingToPeerAddr(second.String())
	})
	tor.AddPeers([]PeerInfo{{Addr: third}})
	cl.lock()
	pending := tor.peers.Len()
	cl.unlock()
	require.Equal(t, 1, pending, "third dial must wait for the global slot")

	key := retryDeadAddr.String()
	eventually(t, 4*time.Second, "fan-out initiated and settled third dial", func() bool {
		cl.lock()
		defer cl.unlock()
		_, parked := tor.failedPeers[third.String()]
		return parked && !tor.connectingToPeerAddr(third.String())
	})
	cl.lock()
	defer cl.unlock()
	_, halfOpen := tor.halfOpen[key]
	require.False(t, halfOpen, "parked address must not be redialed by fan-out")
	require.Contains(t, tor.failedPeers, key, "parked entry must survive fan-out")
}

// Parked entries are not part of the ready reserve: wantPeers and NumWant must
// behave as though the reserve is empty, so the next announce asks for peers.
// Parks more than TorrentPeersLowWater entries so that counting parked entries
// toward the reserve would flip wantPeers — the test discriminates only above
// the low-water mark.
func TestWantPeersAndNumWantIgnoreParkedPeers(t *testing.T) {
	cl, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = time.Hour
	})
	cl.lock()
	for i := range 64 {
		cl.parkFailedOutgoingPeer(tor, PeerInfo{Addr: retryTestAddrPort(uint16(10000 + i))})
	}
	cl.unlock()
	require.Equal(t, 64, parkedCount(tor))

	tor.cl.lock()
	defer tor.cl.unlock()
	require.Zero(t, tor.peers.Len())
	require.True(t, tor.wantPeers(), "parked failures must not satisfy wantPeers")
	req := tor.announceRequest(tracker.Started, *tor.canonicalShortInfohash())
	require.EqualValues(t, 200, req.NumWant, "tracker must be asked for peers while only parked")
}

// A successful outbound connection must clear its parked history through the
// actual outgoingConnection path, not just the eviction helper.
func TestSuccessClearsFailureHistory(t *testing.T) {
	cl, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = time.Hour
	})
	peerClient, _ := newRetryTestClient(t, nil)
	addrs := peerClient.ListenAddrs()
	require.NotEmpty(t, addrs)
	p := PeerInfo{Addr: addrs[0], Trusted: true}
	key := peerRetryKey(p)
	cl.lock()
	cl.parkFailedOutgoingPeer(tor, p)
	cl.parkFailedOutgoingPeer(tor, p)
	cl.unlock()
	require.Equal(t, 2, parkedFailcount(tor, key))
	cl.retryDueFailedPeer(tor, key)
	eventually(t, 3*time.Second, "outbound handshake clears failed history", func() bool {
		cl.lock()
		defer cl.unlock()
		_, parked := tor.failedPeers[key]
		return !parked && tor.hasPeerConnForAddr(p.Addr)
	})
}

// The watchdog only protects a retry still waiting in the ready pool. A dial
// can wait on the rate limiter and handshakes for longer than the watchdog;
// once half-open, only the actual outcome may change its failure history.
func TestStartedRetryKeepsHistoryUntilDialFinishes(t *testing.T) {
	cl, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = time.Hour
		cfg.DialRateLimiter = rate.NewLimiter(1, 1)
	})
	p := PeerInfo{Addr: retryDeadAddr}
	key := peerRetryKey(p)
	cl.lock()
	cl.parkFailedOutgoingPeer(tor, p)
	cl.unlock()
	// Use up the burst to leave the half-open attempt waiting before dial.
	require.True(t, cl.config.DialRateLimiter.Reserve().OK())
	cl.retryDueFailedPeer(tor, key)
	func() {
		cl.lock()
		defer cl.unlock()
		e := tor.failedPeers[key]
		require.NotNil(t, e)
		require.True(t, e.inFlight)
		require.True(t, tor.connectingToPeerAddr(key))
		require.Nil(t, e.timer, "watchdog must stop once a real dial starts")
	}()
	eventually(t, 3*time.Second, "delayed dial increments failure count", func() bool {
		cl.lock()
		defer cl.unlock()
		e, ok := tor.failedPeers[key]
		return ok && e.failcount == 2
	})
}

func TestQueuedRetryKeepsWatchdogUntilAttemptStarts(t *testing.T) {
	cl, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = time.Hour
	})
	p := PeerInfo{Addr: retryDeadAddr}
	key := peerRetryKey(p)
	cl.lock()
	cl.parkFailedOutgoingPeer(tor, p)
	tor.maxEstablishedConns = 0
	cl.unlock()
	cl.retryDueFailedPeer(tor, key)
	func() {
		cl.lock()
		defer cl.unlock()
		require.NotNil(t, tor.failedPeers[key].timer, "undialed candidate needs a watchdog")
		require.Equal(t, 1, tor.peers.Len())
		tor.maxEstablishedConns = 50
		tor.openNewConns()
		require.True(t, tor.connectingToPeerAddr(key))
		require.Nil(t, tor.failedPeers[key].timer, "watchdog must stop when queued peer starts dialing")
	}()
}

func TestSkippedRetryDoesNotSuppressRediscovery(t *testing.T) {
	cl, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = time.Hour
	})
	p := PeerInfo{Addr: retryDeadAddr, Id: cl.peerID}
	key := peerRetryKey(p)
	cl.lock()
	cl.parkFailedOutgoingPeer(tor, p)
	cl.unlock()
	cl.retryDueFailedPeer(tor, key)
	cl.lock()
	_, parked := tor.failedPeers[key]
	connecting := tor.connectingToPeerAddr(key)
	cl.unlock()
	require.False(t, parked, "a self-peer skipped before dial has no outcome to clear its record")
	require.False(t, connecting)
}

func parkedFailcount(tor *Torrent, key string) int {
	tor.cl.lock()
	defer tor.cl.unlock()
	return tor.failedPeers[key].failcount
}

// The failed-set has explicit capacity: insertion at capacity evicts the entry
// closest to eviction anyway. A capped delay does not bound the set by itself.
func TestFailedSetCapacityEvicts(t *testing.T) {
	cl, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetrySetCapacity = 2
		cfg.OutgoingPeerRetryBaseDelay = time.Hour
	})
	p1 := PeerInfo{Addr: retryTestAddrPort(1)}
	p2 := PeerInfo{Addr: retryTestAddrPort(2)}
	p3 := PeerInfo{Addr: retryTestAddrPort(3)}
	cl.lock()
	cl.parkFailedOutgoingPeer(tor, p1) // failcount 1
	cl.parkFailedOutgoingPeer(tor, p2) // failcount 1
	cl.parkFailedOutgoingPeer(tor, p1) // failcount 2: p1 is closest to eviction
	require.Len(t, tor.failedPeers, 2)
	_, p1Present := tor.failedPeers[peerRetryKey(p1)]
	require.True(t, p1Present, "higher failcount entry should not be the capacity victim")
	cl.parkFailedOutgoingPeer(tor, p3)
	require.Len(t, tor.failedPeers, 2)
	require.NotContains(t, tor.failedPeers, peerRetryKey(p1), "closest-to-eviction entry must be evicted at capacity")
	cl.unlock()
}

// Dropping the torrent releases timers and records.
func TestDropReleasesRetryState(t *testing.T) {
	_, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = time.Hour
	})
	tor.AddPeers([]PeerInfo{{Addr: retryDeadAddr}})
	eventually(t, 2*time.Second, "peer parked", func() bool { return parkedCount(tor) == 1 })
	tor.Drop()
	require.Zero(t, parkedCount(tor))
	// Timer callbacks firing after the drop must be no-ops.
	time.Sleep(20 * time.Millisecond)
}

// With the retry disabled the library keeps its upstream discard behaviour.
func TestDisabledRetryKeepsUpstreamBehaviour(t *testing.T) {
	_, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.DisableOutgoingPeerRetry = true
	})
	tor.AddPeers([]PeerInfo{{Addr: retryDeadAddr}})
	eventually(t, 2*time.Second, "failed candidate discarded", func() bool {
		tor.cl.lock()
		defer tor.cl.unlock()
		return tor.peers.Len() == 0
	})
	time.Sleep(50 * time.Millisecond)
	require.Zero(t, parkedCount(tor))
}
