package kv

import (
	mysql "TicketX/internal/db"
	"TicketX/internal/labgob"
	"TicketX/internal/labrpc"
	"TicketX/internal/persister"
	"TicketX/internal/raft"
	"TicketX/internal/rpc"
	"fmt"
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
	db          *mysql.DB
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

func (kv *KvServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()

	op := Op{
		Type:      Get,
		Key:       args.Key,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}

	index, _, ok := kv.rf.Start(op)

	if !ok {
		reply.Success = false
		kv.mu.Unlock()
		return
	}

	//等待，确认指令真的被执行了(暂时这么写)
	ch := make(chan result, 1)
	kv.getCh[index] = ch

	kv.mu.Unlock()

	res := <-ch
	reply.Value = res.Value
	reply.Success = res.Success

}

func (kv *KvServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()

	op := Op{
		Type:      Put,
		Key:       args.Key,
		Value:     args.Value,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}
	//建立好op传给raft
	index, _, ok := kv.rf.Start(op)
	if !ok {
		kv.mu.Unlock()
		reply.Success = false
		return
	}

	//等待，确认指令真的被执行了
	ch := make(chan result, 1)
	kv.waitCh[index] = ch

	kv.mu.Unlock()

	res := <-ch

	reply.Success = res.Success

}

func (kv *KvServer) GetRaft() *raft.Raft {
	return kv.rf
}
func (kv *KvServer) LoadTrainDataFromDB() error {
	// 查询 train_inventory 表，获取所有车次和票数
	rows, err := kv.db.Conn().Query("SELECT train_id, date, tickets FROM train_inventory")
	if err != nil {
		return fmt.Errorf("Error loading train data from MySQL: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var trainID, date string
		var tickets int
		if err := rows.Scan(&trainID, &date, &tickets); err != nil {
			return fmt.Errorf("Error scanning row: %v", err)
		}

		// 构造 Key 和 Value
		key := trainID + ":" + date
		value := fmt.Sprintf("%d", tickets)

		// 将数据存入内存中的 kv 存储
		kv.kv[key] = value
		//fmt.Println("已初始化kv%s %s", key, value)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("Error reading rows: %v", err)
	}
	return nil
}
func MakeKVServer(peers []*labrpc.ClientEnd, me int, db *mysql.DB) *KvServer {
	applych := make(chan raft.ApplyMsg)
	persister := persister.MakePersister()

	labgob.Register(Op{})
	kv := &KvServer{}
	kv.db = db
	kv.kv = make(map[string]string)
	kv.applyCh = applych
	kv.rf = raft.MakeRaft(applych, peers, me, persister)
	kv.waitCh = make(map[int]chan result)
	kv.lastRequest = make(map[int]int)
	kv.getCh = make(map[int]chan result)

	go kv.applyLoop() //循环执行命令

	return kv
}
