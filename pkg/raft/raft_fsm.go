package raft

import (
	"encoding/json"
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
)

// FSM implements the Raft FSM (finite state machine).
// It applies log entries and maintains a small in-memory map.
type FSM struct {
	mu   sync.Mutex
	data map[string]interface{}
}

func NewFSM() *FSM {
	return &FSM{data: make(map[string]interface{})}
}

// Apply is called once a log entry is committed.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var cmd map[string]interface{}
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		log.Printf("fsm apply error: %v", err)
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	filename, ok := cmd["filename"].(string)
	if ok {
		f.data[filename] = cmd
	}
	log.Printf("[FSM] applied command: %+v", cmd)
	return nil
}

// Snapshot returns a snapshot of the current FSM data.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	copy := make(map[string]interface{})
	for k, v := range f.data {
		copy[k] = v
	}
	return &fsmSnapshot{store: copy}, nil
}

// Restore loads an FSM snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	copy := make(map[string]interface{})
	if err := json.NewDecoder(rc).Decode(&copy); err != nil {
		return err
	}
	f.mu.Lock()
	f.data = copy
	f.mu.Unlock()
	return nil
}

// --- Snapshot type ---

type fsmSnapshot struct {
	store map[string]interface{}
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.store)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
