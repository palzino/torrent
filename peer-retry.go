package torrent

import (
	"time"
)

// Failed outgoing dial/handshake retry.
//
// An outgoing candidate is removed from the ready reserve the moment it is
// attempted (Torrent.openNewConns pops it), and Client.outgoingConnection
// discards it on failure. On a firewalled swarm where most peers do not answer
// the first probe, the whole ready reserve is burned on each discovery wave and
// the torrent starves until the next tracker announce. This file parks failed
// candidates in a per-torrent cooldown with a bounded exponential backoff, and
// returns them to the ready reserve when their deadline passes.
//
// Parked entries are NOT part of the ready reserve: Torrent.wantPeers counts
// only t.peers, so tracker demand (WantPeers/NumWant) is unaffected, and
// noLongerHalfOpen's eager fan-out to every torrent cannot redial a parked
// address before its deadline.

const (
	defaultOutgoingPeerRetryMaxFailcount = 4
	defaultOutgoingPeerRetryBaseDelay    = time.Minute
	defaultOutgoingPeerRetryMaxDelay     = 15 * time.Minute
	defaultOutgoingPeerRetrySetCapacity  = 1024
)

// failedPeerEntry is a parked candidate. Keyed by peer address string in
// Torrent.failedPeers.
type failedPeerEntry struct {
	peerInfo  PeerInfo
	failcount int
	nextAt    time.Time
	timer     *time.Timer
	// The deadline passed and the candidate was returned to the ready
	// reserve; the record is kept (suppression continues) so the attempt's
	// outcome can continue the failcount. Cleared when the attempt parks
	// again. A watchdog evicts an inFlight record whose attempt never
	// concluded, so a lost candidate cannot suppress rediscovery forever.
	inFlight bool
}

// peerRetryKey is the canonical address key, matching the halfOpen map key.
func peerRetryKey(p PeerInfo) string {
	return p.Addr.String()
}

// normalizeOutgoingPeerRetry fills zero-valued retry settings so configs built
// without NewDefaultClientConfig still get bounded behaviour.
func (cfg *ClientConfig) normalizeOutgoingPeerRetry() {
	if cfg.OutgoingPeerRetryMaxFailcount == 0 {
		cfg.OutgoingPeerRetryMaxFailcount = defaultOutgoingPeerRetryMaxFailcount
	}
	if cfg.OutgoingPeerRetryBaseDelay == 0 {
		cfg.OutgoingPeerRetryBaseDelay = defaultOutgoingPeerRetryBaseDelay
	}
	if cfg.OutgoingPeerRetryMaxDelay == 0 {
		cfg.OutgoingPeerRetryMaxDelay = defaultOutgoingPeerRetryMaxDelay
	}
	if cfg.OutgoingPeerRetrySetCapacity == 0 {
		cfg.OutgoingPeerRetrySetCapacity = defaultOutgoingPeerRetrySetCapacity
	}
}

func (cl *Client) outgoingPeerRetryDeadline(failcount int) time.Time {
	// Bounded exponential: base << (failcount-1), capped. The shift is bounded
	// by MaxFailcount, but clamp the shift width defensively anyway.
	shift := min(failcount-1, 16)
	d := cl.config.OutgoingPeerRetryBaseDelay * (1 << max(shift, 0))
	return time.Now().Add(min(d, cl.config.OutgoingPeerRetryMaxDelay))
}

// parkFailedOutgoingPeer records an ordinary outbound attempt that failed to
// dial or handshake. Must be called under the Client lock, and before the
// half-open slot is released: noLongerHalfOpen fans openNewConns out to every
// torrent, so the cooling record must already be installed when that happens.
func (cl *Client) parkFailedOutgoingPeer(t *Torrent, p PeerInfo) {
	if cl.config.DisableOutgoingPeerRetry || t.closed.IsSet() {
		return
	}
	key := peerRetryKey(p)
	e, ok := t.failedPeers[key]
	if !ok {
		if len(t.failedPeers) >= cl.config.OutgoingPeerRetrySetCapacity {
			t.evictOneFailedPeerLocked()
		}
		e = new(failedPeerEntry)
		t.failedPeers[key] = e
	}
	e.failcount++
	e.peerInfo = p
	e.inFlight = false
	if e.failcount > cl.config.OutgoingPeerRetryMaxFailcount {
		cl.evictFailedPeerLocked(t, key)
		torrent.Add("outgoing peer retries evicted (failcount exceeded)", 1)
		return
	}
	e.nextAt = cl.outgoingPeerRetryDeadline(e.failcount)
	if e.timer != nil {
		e.timer.Stop()
	}
	e.timer = time.AfterFunc(time.Until(e.nextAt), func() {
		cl.retryDueFailedPeer(t, key)
	})
	torrent.Add("outgoing peer dials parked for retry", 1)
}

// retryDueFailedPeer returns a cooled-down candidate to the ready reserve. It
// runs from a timer goroutine and takes the Client lock itself. The record
// stays parked (inFlight) so the attempt's failure continues its failcount;
// only success, watchdog, or teardown remove it otherwise. Reentry goes
// through the existing openNewConns path, so retries share the dial rate
// limiter and half-open caps like any other candidate.
func (cl *Client) retryDueFailedPeer(t *Torrent, key string) {
	cl.lock()
	defer cl.unlock()
	e, ok := t.failedPeers[key]
	if !ok || t.closed.IsSet() {
		return
	}
	if t.hasPeerConnForAddr(e.peerInfo.Addr) || t.connectingToPeerAddr(key) {
		// Reached by some other path; drop the record. A fresh failure will
		// park again.
		cl.evictFailedPeerLocked(t, key)
		return
	}
	e.inFlight = true
	if e.timer != nil {
		e.timer.Stop()
		e.timer = nil
	}
	// Watchdog: if the attempt never concludes (the ready candidate was
	// trimmed without being dialed and the record went stale), forget it so
	// rediscovery is not suppressed forever.
	watchdogDelay := 2 * max(cl.config.NominalDialTimeout, 5*time.Second)
	e.timer = time.AfterFunc(watchdogDelay, func() {
		cl.lock()
		defer cl.unlock()
		if e, ok := t.failedPeers[key]; ok && e.inFlight {
			cl.evictFailedPeerLocked(t, key)
			torrent.Add("outgoing peer retries forgotten (watchdog)", 1)
		}
	})
	t.peers.AddReturningReplacedPeer(e.peerInfo)
	t.openNewConns()
	cl.event.Broadcast()
	torrent.Add("outgoing peer retries issued", 1)
}

// clearFailedPeers is torrent teardown: stop timers and release records. Must
// be called under the Client lock.
func (t *Torrent) clearFailedPeers() {
	for key, e := range t.failedPeers {
		if e.timer != nil {
			e.timer.Stop()
		}
		delete(t.failedPeers, key)
	}
}

// evictFailedPeerLocked removes one record, stopping its timer.
func (cl *Client) evictFailedPeerLocked(t *Torrent, key string) {
	if e, ok := t.failedPeers[key]; ok {
		if e.timer != nil {
			e.timer.Stop()
		}
		delete(t.failedPeers, key)
	}
}

// evictOneFailedPeerLocked makes room when the failed-set is at capacity.
// Prefers the entry closest to eviction anyway (highest failcount), tie-broken
// by the earliest next deadline. Capacity enforcement is explicit: a capped
// backoff delay does not by itself bound the map.
func (t *Torrent) evictOneFailedPeerLocked() {
	var key string
	var victim *failedPeerEntry
	for k, e := range t.failedPeers {
		if victim == nil || e.failcount > victim.failcount || (e.failcount == victim.failcount && e.nextAt.Before(victim.nextAt)) {
			victim = e
			key = k
		}
	}
	if victim != nil {
		if victim.timer != nil {
			victim.timer.Stop()
		}
		delete(t.failedPeers, key)
		torrent.Add("outgoing peer retry set evicted (capacity)", 1)
	}
}

// retryCooling reports whether the address is parked. If it is, the record's
// peer metadata is refreshed with p (same address, possibly newer flags) while
// the deadline and failcount are preserved.
func (t *Torrent) retryCooling(p PeerInfo) bool {
	e, ok := t.failedPeers[peerRetryKey(p)]
	if !ok {
		return false
	}
	e.peerInfo = p
	torrent.Add("peer additions suppressed during cooldown", 1)
	return true
}

// numFailedPeers is for tests and diagnostics.
func (t *Torrent) numFailedPeers() int {
	return len(t.failedPeers)
}
