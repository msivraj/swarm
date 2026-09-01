//go:build fdb

// This file adds the real, build-tagged FoundationDB adapter behind the same
// store.Store interface memStore/shardedMemStore implement (see #157's fork
// (d) resolution): the `fdb` build tag keeps libfdb/CGo out of the hermetic
// gate — go build/go test without -tags fdb never compile this file — while
// still giving the control plane a real, ACID, durable backend to run
// against a live FoundationDB cluster.
//
// Every operation runs inside one db.Transact call, which gives FDB's usual
// guarantees (serializable isolation, automatic retry on conflict) for free;
// this file adds no decision logic of its own, only translation between
// store.Store calls and FDB reads/writes — the same "the store persists,
// cores decide" boundary memStore holds.
//
// Key layout, all under a caller-supplied prefix subspace (so tests can
// isolate — see NewFDBStoreWithPrefix):
//
//	(prefix, "job", jobID)                -> gob(model.JobSpec)
//	(prefix, "queue", cellID, seq)         -> gob(model.Task)      // FIFO member
//	(prefix, "qseq", cellID)               -> 8-byte big-endian    // next seq
//	(prefix, "taskjob", taskID)            -> jobID (string)       // learned mapping
//	(prefix, "result", jobID, seq)         -> gob(model.TaskResult) // append-only
//	(prefix, "rseq", jobID)                -> 8-byte big-endian    // next seq
//	(prefix, "agg", jobID)                 -> gob(model.Aggregate)
//	(prefix, "registry")                   -> gob(registry.Registry)
//
// The FIFO queue and the (non-deduplicating) result log both use the same
// pattern: a per-key monotonic sequence counter, read and incremented inside
// the same transaction that writes the new member, so ordering is exact and
// race-free under FDB's serializable isolation — simpler than a
// versionstamp and sufficient for correctness (see the package doc's
// "correctness over cleverness" note). DequeueTask range-reads the front
// (lowest-seq) key of a cell's queue subspace and clears it; RequeueTask is
// exactly EnqueueTask (append to the back) by construction, since both take
// the next sequence number in order.
package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	"github.com/msivraj/swarm/internal/core/registry"
	"github.com/msivraj/swarm/internal/model"
)

// apiVersionOnce ensures fdb.MustAPIVersion is called at most once per
// process, as the binding requires.
var apiVersionOnce sync.Once

// fdbAPIVersion is the FDB API version this adapter is built against: 730
// selects the 7.3.x client protocol, matching the installed libfdb_c (FDB
// 7.3.79). It must never exceed the installed library's version.
const fdbAPIVersion = 730

// fdbStore is a store.Store backed by a live FoundationDB cluster. Every
// method opens (or reuses) one FDB transaction via db.Transact, giving ACID
// semantics and automatic conflict retry for free.
type fdbStore struct {
	db   fdb.Database
	root subspace.Subspace

	jobs     subspace.Subspace
	queue    subspace.Subspace
	qseq     subspace.Subspace
	taskJob  subspace.Subspace
	results  subspace.Subspace
	rseq     subspace.Subspace
	aggs     subspace.Subspace
	registry fdb.Key
}

// var _ store.Store = (*fdbStore)(nil) — asserted below via the package's
// own Store type, since this file already lives in package store.
var _ Store = (*fdbStore)(nil)

// NewFDBStore opens a Store backed by the FoundationDB cluster described by
// the default cluster file (/etc/foundationdb/fdb.cluster, or $FDB_CLUSTER_FILE
// if the client library honors it), with no key prefix. Equivalent to
// NewFDBStoreWithPrefix("", "").
func NewFDBStore() (Store, error) {
	return NewFDBStoreWithPrefix("", "")
}

// NewFDBStoreWithPrefix opens a Store backed by the FoundationDB cluster
// named by clusterFile (the default cluster file is used when clusterFile is
// empty), with every key namespaced under prefix. A distinct prefix per
// caller (e.g. per test) makes runs against a shared live cluster isolated
// and repeatable — see fdb_test.go.
func NewFDBStoreWithPrefix(clusterFile, prefix string) (Store, error) {
	apiVersionOnce.Do(func() { fdb.MustAPIVersion(fdbAPIVersion) })

	db, err := fdb.OpenDatabase(clusterFile)
	if err != nil {
		return nil, err
	}

	root := subspace.Sub(prefix)
	return &fdbStore{
		db:       db,
		root:     root,
		jobs:     root.Sub("job"),
		queue:    root.Sub("queue"),
		qseq:     root.Sub("qseq"),
		taskJob:  root.Sub("taskjob"),
		results:  root.Sub("result"),
		rseq:     root.Sub("rseq"),
		aggs:     root.Sub("agg"),
		registry: root.Pack(tuple.Tuple{"registry"}),
	}, nil
}

// gobEncodeValue gob-encodes v into a fresh byte slice.
func gobEncodeValue(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// gobDecodeValue decodes data into v, the reverse of gobEncodeValue.
func gobDecodeValue(data []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

// nextSeq reads the 8-byte big-endian counter at key (0 if absent), writes
// key back as counter+1, and returns counter — the sequence number the
// caller should use for the item it is about to write. Called inside an
// already-open transaction, so the read-modify-write is atomic with the
// caller's own write under FDB's serializable isolation.
func nextSeq(tr fdb.Transaction, key fdb.Key) (int64, error) {
	raw, err := tr.Get(key).Get()
	if err != nil {
		return 0, err
	}
	var cur int64
	if len(raw) == 8 {
		cur = int64(binary.BigEndian.Uint64(raw))
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(cur+1))
	tr.Set(key, buf)
	return cur, nil
}

func (s *fdbStore) PutJob(spec model.JobSpec) error {
	if spec.ID == "" {
		return ErrEmptyJobID
	}
	data, err := gobEncodeValue(spec)
	if err != nil {
		return err
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(s.jobs.Pack(tuple.Tuple{string(spec.ID)}), data)
		return nil, nil
	})
	return err
}

func (s *fdbStore) GetJob(id model.JobID) (model.JobSpec, bool, error) {
	if id == "" {
		return model.JobSpec{}, false, ErrEmptyJobID
	}
	res, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		raw, err := tr.Get(s.jobs.Pack(tuple.Tuple{string(id)})).Get()
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, nil
		}
		var spec model.JobSpec
		if err := gobDecodeValue(raw, &spec); err != nil {
			return nil, err
		}
		return &spec, nil
	})
	if err != nil {
		return model.JobSpec{}, false, err
	}
	if res == nil {
		return model.JobSpec{}, false, nil
	}
	return *res.(*model.JobSpec), true, nil
}

func (s *fdbStore) EnqueueTask(cell model.CellID, t model.Task) error {
	if t.ID == "" {
		return ErrEmptyTaskID
	}
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return nil, s.enqueue(tr, cell, t)
	})
	return err
}

// enqueue appends t to the back of cell's queue and records t's TaskID ->
// JobID mapping, inside an already-open transaction. Shared by
// EnqueueTask/RequeueTask, which behave identically (both append to the
// back — see the package doc).
func (s *fdbStore) enqueue(tr fdb.Transaction, cell model.CellID, t model.Task) error {
	seq, err := nextSeq(tr, s.qseq.Pack(tuple.Tuple{string(cell)}))
	if err != nil {
		return err
	}
	data, err := gobEncodeValue(t)
	if err != nil {
		return err
	}
	tr.Set(s.queue.Sub(string(cell)).Pack(tuple.Tuple{seq}), data)
	tr.Set(s.taskJob.Pack(tuple.Tuple{string(t.ID)}), []byte(t.JobID))
	return nil
}

func (s *fdbStore) DequeueTask(cell model.CellID) (model.Task, bool, error) {
	res, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		cellQueue := s.queue.Sub(string(cell))
		rows, err := tr.GetRange(cellQueue, fdb.RangeOptions{Limit: 1}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, nil
		}
		var t model.Task
		if err := gobDecodeValue(rows[0].Value, &t); err != nil {
			return nil, err
		}
		tr.Clear(rows[0].Key)
		return &t, nil
	})
	if err != nil {
		return model.Task{}, false, err
	}
	if res == nil {
		return model.Task{}, false, nil
	}
	return *res.(*model.Task), true, nil
}

func (s *fdbStore) RequeueTask(cell model.CellID, t model.Task) error {
	if t.ID == "" {
		return ErrEmptyTaskID
	}
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return nil, s.enqueue(tr, cell, t)
	})
	return err
}

func (s *fdbStore) PutResult(r model.TaskResult) error {
	if r.TaskID == "" {
		return ErrEmptyTaskID
	}
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		raw, err := tr.Get(s.taskJob.Pack(tuple.Tuple{string(r.TaskID)})).Get()
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, ErrUnknownTask
		}
		jobID := string(raw)

		seq, err := nextSeq(tr, s.rseq.Pack(tuple.Tuple{jobID}))
		if err != nil {
			return nil, err
		}
		data, err := gobEncodeValue(r)
		if err != nil {
			return nil, err
		}
		tr.Set(s.results.Sub(jobID).Pack(tuple.Tuple{seq}), data)
		return nil, nil
	})
	return err
}

func (s *fdbStore) ResultsForJob(id model.JobID) ([]model.TaskResult, error) {
	if id == "" {
		return nil, ErrEmptyJobID
	}
	res, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		rows, err := tr.GetRange(s.results.Sub(string(id)), fdb.RangeOptions{}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		out := make([]model.TaskResult, 0, len(rows))
		for _, row := range rows {
			var r model.TaskResult
			if err := gobDecodeValue(row.Value, &r); err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return res.([]model.TaskResult), nil
}

func (s *fdbStore) PutAggregate(a model.Aggregate) error {
	if a.JobID == "" {
		return ErrEmptyJobID
	}
	data, err := gobEncodeValue(a)
	if err != nil {
		return err
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(s.aggs.Pack(tuple.Tuple{string(a.JobID)}), data)
		return nil, nil
	})
	return err
}

func (s *fdbStore) GetAggregate(id model.JobID) (model.Aggregate, bool, error) {
	if id == "" {
		return model.Aggregate{}, false, ErrEmptyJobID
	}
	res, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		raw, err := tr.Get(s.aggs.Pack(tuple.Tuple{string(id)})).Get()
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, nil
		}
		var a model.Aggregate
		if err := gobDecodeValue(raw, &a); err != nil {
			return nil, err
		}
		return &a, nil
	})
	if err != nil {
		return model.Aggregate{}, false, err
	}
	if res == nil {
		return model.Aggregate{}, false, nil
	}
	return *res.(*model.Aggregate), true, nil
}

func (s *fdbStore) Registry() registry.Registry {
	res, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		raw, err := tr.Get(s.registry).Get()
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, nil
		}
		var reg registry.Registry
		if err := gobDecodeValue(raw, &reg); err != nil {
			return nil, err
		}
		return &reg, nil
	})
	if err != nil || res == nil {
		// Registry() carries no error return in the Store interface (see
		// store.go); an absent or unreadable registry is treated the same
		// as memStore's zero value — a valid, empty Registry.
		return registry.Registry{}
	}
	return *res.(*registry.Registry)
}

func (s *fdbStore) SetRegistry(reg registry.Registry) error {
	data, err := gobEncodeValue(reg)
	if err != nil {
		return err
	}
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(s.registry, data)
		return nil, nil
	})
	return err
}
