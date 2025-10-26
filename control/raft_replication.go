package main

import (
	"archive/zip"
	"io"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// replicateLogEntry sends a single log entry to all peers and waits for majority replication.
func (n *RaftNode) replicateLogEntry(entry LogEntry) bool {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		log.Printf("[%s] replicateLogEntry called but not leader\n", n.ID)
		return false
	}
	term := n.persistent.CurrentTerm
	n.mu.Unlock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 1 // leader itself counts
	totalPeers := len(n.Peers)
	majority := (totalPeers+1)/2 + 1

	for _, peerAddr := range n.Peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				return
			}
			defer client.Close()

			args := AppendEntriesArgs{
				Term:     term,
				LeaderID: n.ID,
				Entries:  []LogEntry{entry},
			}
			var reply AppendEntriesReply

			// set timeout manually
			done := make(chan error, 1)
			go func() { done <- client.Call("RaftRPC.AppendEntries", args, &reply) }()
			select {
			case err = <-done:
			case <-time.After(1 * time.Second):
				err = rpc.ErrShutdown
			}
			if err != nil {
				return
			}

			if reply.Success {
				mu.Lock()
				successCount++
				mu.Unlock()
				log.Printf("[%s] replication to %s succeeded (count=%d/%d required=%d)",
					n.ID, peer, successCount, totalPeers+1, majority)
			}
		}(peerAddr)
	}

	wg.Wait()

	if successCount >= majority {
		log.Printf("[%s] committed up to index %d", n.ID, entry.Index)
		return true
	}
	log.Printf("[%s] replication failed for index %d (%d/%d)", n.ID, entry.Index, successCount, totalPeers+1)
	return false
}

// replicateLogEntries broadcasts the given entries to all followers.
func (n *RaftNode) replicateLogEntries(entries []LogEntry) bool {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return false
	}
	term := n.persistent.CurrentTerm
	leaderID := n.ID
	n.mu.Unlock()

	N := 1 + len(n.Peers)
	required := N/2 + 1

	var successCount int32 = 1 // leader already has the entry
	prevIndex := 0
	if len(entries) > 0 {
		prevIndex = entries[0].Index - 1
	}

	for _, peer := range n.Peers {
		go func(peerAddr string) {
			client, err := rpc.Dial("tcp", peerAddr)
			if err != nil {
				return
			}
			defer client.Close()

			prevTerm := 0
			if prevIndex > 0 {
				n.mu.Lock()
				for i := len(n.persistent.Log) - 1; i >= 0; i-- {
					if n.persistent.Log[i].Index == prevIndex {
						prevTerm = n.persistent.Log[i].Term
						break
					}
				}
				n.mu.Unlock()
			}

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: n.volatile.commitIndex,
			}
			var reply AppendEntriesReply
			callErr := client.Call("RaftRPC.AppendEntries", args, &reply)
			if callErr != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.persistent.CurrentTerm {
				n.persistent.CurrentTerm = reply.Term
				n.role = Follower
				return
			}

			if reply.Success {
				atomic.AddInt32(&successCount, 1)
				n.matchIndex[peerAddr] = reply.MatchIndex
				n.nextIndex[peerAddr] = reply.MatchIndex + 1
			} else {
				if n.nextIndex[peerAddr] > 1 {
					n.nextIndex[peerAddr]--
				}
			}
		}(peer)
	}

	// wait for majority (max ~500ms)
	waited := 0
	for waited < 500 {
		if int(atomic.LoadInt32(&successCount)) >= required {
			n.mu.Lock()
			last := n.persistent.Log[len(n.persistent.Log)-1].Index
			if last > n.volatile.commitIndex {
				n.volatile.commitIndex = last
				// immediately apply entries on leader
				n.applyEntries()
			}
			n.mu.Unlock()
			return true
		}
		time.Sleep(25 * time.Millisecond)
		waited += 25
	}
	return false
}

// =======================
// Helpers: download & unzip
// =======================

func downloadToTemp(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	tmpFile, err := os.CreateTemp("", "site_*.zip")
	if err != nil {
		return "", err
	}
	defer tmpFile.Close()

	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		os.Remove(tmpFile.Name())
		return "", err
	}
	return tmpFile.Name(), nil
}

func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		fpath := filepath.Join(dest, f.Name)

		// flatten if nested top-level dir (e.g., "sample_site/")
		if parts := strings.SplitN(f.Name, string(os.PathSeparator), 2); len(parts) == 2 {
			// remove first folder from path
			fpath = filepath.Join(dest, parts[1])
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(fpath, 0755)
			continue
		}

		if err := os.MkdirAll(filepath.Dir(fpath), 0755); err != nil {
			return err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return err
		}

		_, err = io.Copy(outFile, rc)

		outFile.Close()
		rc.Close()

		if err != nil {
			return err
		}
	}
	return nil
}

// applyEntries moves entries from lastApplied+1 .. commitIndex to applyCh (state machine)
func (n *RaftNode) applyEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.volatile.lastApplied < n.volatile.commitIndex {
		n.volatile.lastApplied++
		idx := n.volatile.lastApplied

		var entry LogEntry
		found := false
		for _, e := range n.persistent.Log {
			if e.Index == idx {
				entry = e
				found = true
				break
			}
		}
		if !found {
			log.Printf("[%s] applyEntries: entry index %d not found in log (commitIndex=%d)\n",
				n.ID, idx, n.volatile.commitIndex)
			continue
		}

		cmd := entry.Command

		// ---- Worker/peer registration ----
		if strings.HasPrefix(cmd, "register|") {
			parts := strings.SplitN(cmd, "|", 4)
			if len(parts) == 4 {
				workerID := parts[1]
				host := parts[2]
				port, _ := strconv.Atoi(parts[3])
				n.Workers[workerID] = WorkerInfo{ID: workerID, Host: host, Port: port}
				n.WorkerLastSeen[workerID] = time.Now()
				log.Printf("[%s] Applied register: %s -> %s:%d\n", n.ID, workerID, host, port)
			}
		}

		// ---- Deploy command ----
		if strings.HasPrefix(cmd, "deploy|") {
			parts := strings.SplitN(cmd, "|", 6)
			if len(parts) != 6 {
				log.Printf("[%s] malformed deploy command: %s\n", n.ID, cmd)
			} else {
				user := parts[1]
				site := parts[2]
				fileURL := parts[3]

				go func(user, site, fileURL string) {
					targetDir := filepath.Join(n.SitesDir, user, site)
					if err := os.MkdirAll(targetDir, 0o755); err != nil {
						log.Printf("[%s] deploy: mkdir failed: %v\n", n.ID, err)
						return
					}

					log.Printf("[%s] Deploy: downloading %s for %s/%s -> %s\n",
						n.ID, fileURL, user, site, targetDir)

					tmpZip, err := downloadToTemp(fileURL)
					if err != nil {
						log.Printf("[%s] deploy: download failed: %v\n", n.ID, err)
						return
					}
					defer os.Remove(tmpZip)

					// ✅ Extract to a temporary folder first
					tmpExtract := targetDir + "_tmp"
					os.RemoveAll(tmpExtract)
					if err := os.MkdirAll(tmpExtract, 0o755); err != nil {
						log.Printf("[%s] deploy: mkdir tmp failed: %v\n", n.ID, err)
						return
					}

					if err := unzip(tmpZip, tmpExtract); err != nil {
						log.Printf("[%s] deploy: unzip failed: %v\n", n.ID, err)
						return
					}

					// ✅ Flatten if there's a single subdirectory (like "sample_site")
					entries, _ := os.ReadDir(tmpExtract)
					if len(entries) == 1 && entries[0].IsDir() {
						inner := filepath.Join(tmpExtract, entries[0].Name())
						files, _ := os.ReadDir(inner)
						for _, f := range files {
							src := filepath.Join(inner, f.Name())
							dst := filepath.Join(targetDir, f.Name())
							os.Rename(src, dst)
						}
					} else {
						// multiple files: move everything
						for _, f := range entries {
							src := filepath.Join(tmpExtract, f.Name())
							dst := filepath.Join(targetDir, f.Name())
							os.Rename(src, dst)
						}
					}

					os.RemoveAll(tmpExtract)
					log.Printf("[%s] deploy: site available at %s (user=%s site=%s)\n",
						n.ID, targetDir, user, site)
				}(user, site, fileURL)
			}
		}

		// ---- Send entry to applyCh (non-blocking) ----
		select {
		case n.applyCh <- entry:
		default:
			go func(en LogEntry) { n.applyCh <- en }(entry)
		}
	}
}
