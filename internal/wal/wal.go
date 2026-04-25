package wal

import (
	types "TicketX/internal/type"
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type Wal struct {
	mu   sync.Mutex
	file *os.File
	path string
}

type WalEntry struct {
	Index   int64
	Term    int64
	Command []byte
}

func NewWal(path string) *Wal {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	return &Wal{
		file: file,
		path: path,
	}
}
func (w *Wal) Append(entry WalEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	_, err = w.file.Write(append(data, '\n'))
	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *Wal) LoadAll() []types.LogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	file, _ := os.Open(w.path)
	defer file.Close()

	var logs []types.LogEntry

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()

		var e WalEntry
		json.Unmarshal(line, &e)
		logs = append(logs, types.LogEntry{
			Index:   int32(e.Index),
			Term:    int32(e.Term),
			Command: e.Command,
		})
	}
	return logs
}
func (w *Wal) Exists() bool {
	_, err := os.Stat(w.path)
	return err == nil
}
