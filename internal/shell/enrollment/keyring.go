package enrollment

import (
	"sync"

	"github.com/msivraj/swarm/internal/model"
)

// Keyring is the in-memory signing-key registry a verifier consults before
// dispatch: it maps an admitted SpiffeID to the public key it enrolled
// with, so enrollment.VerifySignature (core) can be called against a key
// the verifier actually trusts, rather than one attached to the workload
// itself (which an attacker could substitute). Enroller populates it on a
// successful, non-blacklisted Enroll; VerifyWorkload consults it.
//
// A real deployment might back this with a replicated store so every
// verifier/dispatcher sees the same keys; the in-memory form here is
// sufficient for a single process and for tests.
type Keyring struct {
	mu   sync.RWMutex
	keys map[model.SpiffeID]model.PubKey
}

// NewKeyring returns an empty Keyring.
func NewKeyring() *Keyring {
	return &Keyring{keys: make(map[model.SpiffeID]model.PubKey)}
}

// Register distributes id's signing public key to the registry, overwriting
// any previously registered key for id. Safe for concurrent use.
func (k *Keyring) Register(id model.SpiffeID, key model.PubKey) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.keys[id] = key
}

// Lookup returns the public key registered for id, if any.
func (k *Keyring) Lookup(id model.SpiffeID) (model.PubKey, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	key, ok := k.keys[id]
	return key, ok
}

// Revoke removes id's registered key, e.g. once an identity is blacklisted
// — a revoked identity has no key left to verify workloads against, so
// VerifyWorkload refuses it.
func (k *Keyring) Revoke(id model.SpiffeID) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.keys, id)
}
