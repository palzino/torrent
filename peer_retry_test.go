package torrent

import (
	"net/netip"
	"testing"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/tracker"
	"github.com/stretchr/testify/require"
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
	_, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = time.Hour // no due retries during the test
	})
	tor.AddPeers([]PeerInfo{{Addr: retryDeadAddr}})
	eventually(t, 2*time.Second, "peer parked", func() bool { return parkedCount(tor) == 1 })

	// Pump the paths that fan out openNewConns: explicit calls and fresh
	// discoveries releasing half-open slots.
	tor.openNewConns()
	tor.AddPeers([]PeerInfo{{Addr: retryTestAddrPort(2)}})
	tor.openNewConns()

	key := retryDeadAddr.String()
	eventually(t, 2*time.Second, "second peer settled", func() bool {
		return parkedCount(tor) <= 2
	})
	tor.cl.lock()
	defer tor.cl.unlock()
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

// A successful connection clears failure history: a later failure starts the
// count from one. (Mirrors what outgoingConnection does on success.)
func TestSuccessClearsFailureHistory(t *testing.T) {
	cl, tor := newRetryTestClient(t, func(cfg *ClientConfig) {
		cfg.OutgoingPeerRetryBaseDelay = time.Hour
	})
	p := PeerInfo{Addr: retryDeadAddr}
	key := peerRetryKey(p)
	cl.lock()
	cl.parkFailedOutgoingPeer(tor, p)
	cl.parkFailedOutgoingPeer(tor, p)
	require.Equal(t, 2, tor.failedPeers[key].failcount)
	cl.evictFailedPeerLocked(tor, key)
	cl.parkFailedOutgoingPeer(tor, p)
	require.Equal(t, 1, tor.failedPeers[key].failcount, "history must not survive success eviction")
	cl.unlock()
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
