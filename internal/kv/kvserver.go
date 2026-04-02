package kv

import (
	"TicketX/internal/raft"
	"sync"
)

type KvServer struct {
	mu      sync.Mutex
	kv      map[string]string
	applyCh chan raft.ApplyMsg
	rf      *raft.Raft
}

type OpType string

const (
	Put    OpType = "Put"
	Get    OpType = "Get"
	Delete OpType = "Delete"
)

type Op struct {
	Type  OpType
	Key   string
	Value string
}

func (kv *KvServer) Get(key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.kv[key] //get指令暂时不进入raft
}
func (kv *KvServer) Put(key string, value string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := &Op{
		Type:  Put,
		Key:   key,
		Value: value,
	}
	//建立好op传给raft
	_, _, ok := kv.rf.Start(op)

	if !ok {
		return false
	}
	return true

}

// 接受管道来到msg并执行
func (kv *KvServer) applyLoop() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			switch op.Type {
			case Put:
				kv.kv[op.Key] = op.Value
			}
		}
	}

}

func MakeKVServer() *KvServer {
	applych := make(chan raft.ApplyMsg)
	kv := &KvServer{
		kv:      make(map[string]string),
		applyCh: applych,
		rf:      raft.MakeRaft(applych),
	}

	return kv
}
