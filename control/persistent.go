package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// call once to register types if needed (gob can usually infer, but explicit is safe)
func init() {
	gob.Register(PersistentState{})
	gob.Register(LogEntry{})
}

// stateFilename returns default path for node persistent state
func stateFilename(nodeID string) string {
	// you can change folder if desired
	return fmt.Sprintf("raft_state_%s.gob", nodeID)
}

// persistState writes the PersistentState to disk atomically.
// Caller must hold the node lock (n.mu) if reading/writing n.persistent.
func (n *RaftNode) persistState() error {
	// take copy under lock by caller, or call while holding lock
	filename := n.StateFile
	if filename == "" {
		filename = stateFilename(n.ID)
	}

	// ensure directory exists
	dir := filepath.Dir(filename)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("[%s] persist: mkdir failed: %v\n", n.ID, err)
			// continue, might work if dir exists
		}
	}

	tmp := filename + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	// encode the persistent state
	if err := enc.Encode(n.persistent); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	// fsync to make sure data is on disk
	if err := f.Sync(); err != nil {
		// not fatal, but log
		log.Printf("[%s] persist: sync warning: %v\n", n.ID, err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	// atomic rename
	if err := os.Rename(tmp, filename); err != nil {
		os.Remove(tmp)
		return err
	}
	// success
	return nil
}

// loadState loads persistent state into n.persistent. Caller should hold lock if concurrent.
// If file is missing, it returns nil (no error) and leaves state zero-valued.
func (n *RaftNode) loadState() error {
	filename := n.StateFile
	if filename == "" {
		filename = stateFilename(n.ID)
	}
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		// nothing to load
		return nil
	}
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	var ps PersistentState
	if err := dec.Decode(&ps); err != nil {
		return err
	}
	n.persistent = ps
	return nil
}
