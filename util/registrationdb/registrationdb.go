package registrationdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type RegistrationDB struct {
	mtx  sync.RWMutex
	data map[Registration]Producers
}

type Registration struct {
	Category string
	Key      string
	SubKey   string
}

func (r Registration) String() string {
	return fmt.Sprintf("category:%s key:%s subkey:%s",
		r.Category, r.Key, r.SubKey)
}

type Registrations []Registration

type PeerInfo struct {
	LastUpdate       int64  `json:"-"`
	ID               string `json:"-"`
	RemoteAddress    string `json:"-"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

type Producer struct {
	*PeerInfo
	tombstoned   bool
	tombstonedAt time.Time
}

type Producers []*Producer

func (p Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]",
		p.BroadcastAddress, p.TCPPort, p.HTTPPort)
}

func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Now().Sub(p.tombstonedAt) < lifetime
}

func New() *RegistrationDB {
	return &RegistrationDB{
		data: make(map[Registration]Producers),
	}
}

func (r *RegistrationDB) Debug() map[string][]map[string]interface{} {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range r.data {
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		data[key] = make([]map[string]interface{}, 0)
		for _, p := range producers {
			m := make(map[string]interface{})
			m["id"] = p.ID
			m["hostname"] = p.Hostname
			m["broadcast_address"] = p.BroadcastAddress
			m["tcp_port"] = p.TCPPort
			m["http_port"] = p.HTTPPort
			m["version"] = p.Version
			m["last_update"] = atomic.LoadInt64(&p.LastUpdate)
			m["tombstoned"] = p.tombstoned
			m["tombstoned_at"] = p.tombstonedAt.UnixNano()
			data[key] = append(data[key], m)
		}
	}

	return data
}

// add a registration key
func (r *RegistrationDB) AddRegistration(k Registration) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	_, ok := r.data[k]
	if !ok {
		r.data[k] = make(Producers, 0)
	}
}

// add a producer to a registration
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	producers := r.data[k]
	found := false
	for _, producer := range producers {
		if producer.ID == p.ID {
			found = true
		}
	}
	if found == false {
		r.data[k] = append(producers, p)
	}
	return !found
}

// remove a producer from a registration
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	producers, ok := r.data[k]
	if !ok {
		return false, 0
	}
	removed := false
	cleaned := make(Producers, 0)
	for _, producer := range producers {
		if producer.ID != id {
			cleaned = append(cleaned, producer)
		} else {
			removed = true
		}
	}
	// Note: this leaves keys in the DB even if they have empty lists
	r.data[k] = cleaned
	return removed, len(cleaned)
}

// remove a Registration and all it's producers
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	delete(r.data, k)
}

func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	results := make(Registrations, 0)
	for k := range r.data {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		results = append(results, k)
	}
	return results
}

func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	results := make(Producers, 0)
	for k, producers := range r.data {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		for _, producer := range producers {
			found := false
			for _, p := range results {
				if producer.ID == p.ID {
					found = true
				}
			}
			if found == false {
				results = append(results, producer)
			}
		}
	}
	return results
}

func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	results := make(Registrations, 0)
	for k, producers := range r.data {
		for _, p := range producers {
			if p.ID == id {
				results = append(results, k)
				break
			}
		}
	}
	return results
}

func (r *RegistrationDB) TouchProducer(k Registration, id string) bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	now := time.Now()
	producers, ok := r.data[k]
	if !ok {
		return false
	}
	for _, p := range producers {
		if p.ID == id {
			atomic.StoreInt64(&p.LastUpdate, now.UnixNano())
			return true
		}
	}
	return false
}

func (k Registration) IsMatch(category string, key string, subkey string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && k.Key != key {
		return false
	}
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	return true
}

func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := make(Registrations, 0)
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := make(Producers, 0)
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.LastUpdate))
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		results = append(results, p)
	}
	return results
}
