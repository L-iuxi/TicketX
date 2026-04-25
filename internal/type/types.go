package types

type LogEntry struct {
	Index   int32
	Term    int32
	Command []byte
}
