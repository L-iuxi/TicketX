package kv

import (
	"TicketX/internal/raft"
	"TicketX/internal/rpc"
	"sync"
)

// 把applyloop结果返回给put/get的
type result struct {
	Success bool
	Value   string
}

type KvServer struct {
	mu sync.Mutex
	kv map[string]string

	applyCh chan raft.ApplyMsg  //和raft通信的管道
	waitCh  map[int]chan result //确保put请求成功commit的管道

	getCh map[int]chan result //暂时确保get一致性，多机删

	lastRequest map[int]int //请求者对应的最后一个请求编号
	rf          *raft.Raft
}

type OpType string

const (
	Put    OpType = "Put"
	Get    OpType = "Get"
	Delete OpType = "Delete"
)

// 封装发送给leader，命令的全部信息
type Op struct {
	Type  OpType
	Key   string
	Value string

	ClientId  int //请求者号
	RequestId int //请求号
}

func (kv *KvServer) Get(key string, clientId int, requestId int) string {
	kv.mu.Lock()

	op := &Op{
		Type:      Get,
		Key:       key,
		RequestId: requestId,
		ClientId:  clientId,
	}

	index, _, ok := kv.rf.Start(op)
	if !ok {
		kv.mu.Unlock()
		return ""
	}

	//等待，确认指令真的被执行了(暂时这么写)
	ch := make(chan result, 1)
	kv.getCh[index] = ch

	kv.mu.Unlock()

	res := <-ch
	return res.Value

}

func (kv *KvServer) Put(key string, value string, clientId int, requestId int) bool {
	kv.mu.Lock()

	op := &Op{
		Type:      Put,
		Key:       key,
		Value:     value,
		RequestId: requestId,
		ClientId:  clientId,
	}
	//建立好op传给raft
	index, _, ok := kv.rf.Start(op)
	if !ok {
		kv.mu.Unlock()
		return false
	}

	//等待，确认指令真的被执行了
	ch := make(chan result, 1)
	kv.waitCh[index] = ch

	kv.mu.Unlock()

	res := <-ch
	return res.Success

}

// 接受管道来到msg并执行
func (kv *KvServer) applyLoop() {

	for msg := range kv.applyCh {

		if msg.CommandValid {
			op := msg.Command.(*Op)

			kv.mu.Lock()

			switch op.Type {
			case Put:
				last, ok := kv.lastRequest[op.ClientId]

				if ok && op.RequestId <= last { //重复请求不重复执行只返回
				} else { //未重复请求执行并记录
					kv.kv[op.Key] = op.Value
					kv.lastRequest[op.ClientId] = op.RequestId

				}
				//返回结果给put
				if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
					ch <- result{Success: true}
					delete(kv.waitCh, msg.CommandIndex)
				}
			case Get: //不在乎重复请求
				if ch, ok := kv.getCh[msg.CommandIndex]; ok {
					ch <- result{Value: kv.kv[op.Key]}
					delete(kv.getCh, msg.CommandIndex)
				}
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *KvServer) GetRaft() *raft.Raft {
	return kv.rf
}
func MakeKVServer(peers []*rpc.ClientEnd, me int) *KvServer {
	applych := make(chan raft.ApplyMsg)
	persister := rpc.MakePersister()

	kv := &KvServer{}
	kv.kv = make(map[string]string)
	kv.applyCh = applych
	kv.rf = raft.MakeRaft(applych, peers, me, persister)
	kv.waitCh = make(map[int]chan result)
	kv.lastRequest = make(map[int]int)
	kv.getCh = make(map[int]chan result)

	go kv.applyLoop() //循环执行命令

	return kv
}
