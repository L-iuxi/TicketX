package raft

import (
	"TicketX/internal/labgob"
	"TicketX/internal/persister"
	types "TicketX/internal/type"
	"TicketX/internal/wal"
	"TicketX/proto"
	"bytes"
	"context"

	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 传输消息的结构体
type ApplyMsg struct {
	CommandIndex int64
	Command      interface{} //不关心类型
	CommandValid bool

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int32
	SnapshotIndex int32
}

// 定义server状态
type State string

const (
	Leader    State = "Leader"
	Candidate State = "Candidate"
	Follower  State = "Follower"
)

// Raft结构体
type Raft struct {
	mu            sync.Mutex
	me            int      //当前服务器在peer的下标
	peers         []string //存有所有服务器的组
	clients       []proto.RaftClient
	states        State                //状态
	term          int32                //当前任期号
	vote          int32                //投票给
	persister     *persister.Persister // 持久化状态的对象，用来保存Raft状态，以便在崩溃和重启后恢复
	log           []types.LogEntry     //日志
	nowLeader     int64
	lastHeartbeat time.Time

	commitIndex      int32         //等待提交的最新日志编号
	nextIndex        []int32       //日志同步的位置（从哪里开始同步日志
	lastApply        int32         //上次执行的最后一条日志编号
	applyCh          chan ApplyMsg //与kvserver沟通的渠道
	heartbeat        *time.Timer   //心跳超时
	overElectiontime *time.Timer   //选举超时
	lastSnapIndex    int32         //上次截断日志的位置
	lastSnapTerm     int32         //上次截断日志的任期
	matchIndex       []int32
	snap             []byte
	wal              *wal.Wal

	proto.UnimplementedRaftServer
}

// 请求投票的结构体
type RequestVoteArgs struct {
	Term         int32 //当前任期号
	CandidateId  int32 //候选人id
	LastLogIndex int32 //候选人最新日志的index
	LastLogTerm  int32 //候选人最新日志的任期
}

// 投票给出的回复
type RequestVoteReply struct {
	Term   int32 //当前任期号
	IsVote int32 //是否投票
}

type HeartbeatArgs struct {
	LeaderId          int32
	LeaderTerm        int32
	Entries           []types.LogEntry
	PreLogIndex       int32 //最后对齐位置
	PreLogTerm        int32 //最后对齐位置的任期
	LeaderCommitIndex int32
}

type HeartbeatReply struct {
	Success       bool
	Term          int32
	ConflictIndex int32 //冲突位置
}

type InstallSnapshotArgs struct {
	Term          int32
	LeaderId      int32
	LastSnapIndex int32
	LastSnapTerm  int32
	Data          []byte //snap内容
}

type InstallSnapshotReply struct {
	Term int32
}

func (rf *Raft) GetCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.commitIndex)
}
func (rf *Raft) LastHeartbeat() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastHeartbeat
}
func (rf *Raft) recoverFromWAL() {
	entries := rf.wal.LoadAll()

	for _, e := range entries {
		rf.log = append(rf.log, e)
	}

	rf.commitIndex = rf.lastApply // 或从 wal meta 恢复
}

// 获取当前节点在当前任期是否leader
func (rf *Raft) GetState() (int32, bool) {

	var term int32
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.states == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.term
	return term, isleader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.log)
	e.Encode(rf.vote)
	e.Encode(rf.term)
	e.Encode(rf.lastSnapIndex)
	e.Encode(rf.lastSnapTerm)

	return w.Bytes()

}

// 持久化保存当前raft状态，防止节点崩溃
func (rf *Raft) persist() {
	states := rf.encodeState()
	rf.persister.Save(states, rf.snap)
}

// 解码持久化信息
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any states?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var Log []types.LogEntry
	var Term int32
	var Vote int32
	var SnapIndex int32
	var SnapTerm int32

	if d.Decode(&Log) != nil || d.Decode(&Vote) != nil || d.Decode(&Term) != nil || d.Decode(&SnapIndex) != nil || d.Decode(&SnapTerm) != nil {
		//解码失败
	} else {
		rf.log = Log
		rf.vote = Vote
		rf.term = Term
		rf.lastSnapIndex = SnapIndex
		rf.lastSnapTerm = SnapTerm
	}

}

// how many bytes in Raft's persisted log?
// 读取raft日志中多少bytes
func (rf *Raft) PersistBytes() int32 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int32(rf.persister.RaftStateSize())
}

// 获取
func (rf *Raft) getRealIndex(i int32) int32 {
	return i - rf.lastSnapIndex
}

// 未快照截断的日志长度
func (rf *Raft) getLastIndex() int32 {
	return int32(int(rf.lastSnapIndex) + len(rf.log) - 1)
}

func (rf *Raft) sendRequestVote(server int32, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	res, err := rf.clients[server].RequestVote(ctx, &proto.RequestVoteArgs{
		Term:         int32(args.Term),
		CandidateId:  int32(args.CandidateId),
		LastLogIndex: int32(args.LastLogIndex),
		LastLogTerm:  int32(args.LastLogTerm),
	})
	//fmt.Printf("我收到了票 %d ", reply.IsVote)
	if err != nil {
		return false
	}

	reply.Term = int32(res.Term)
	reply.IsVote = 0
	if res.VoteGranted {
		reply.IsVote = 1
	}
	return true
}

// 接收投票请求，投出票
func (rf *Raft) RequestVote(ctx context.Context, args *proto.RequestVoteArgs) (*proto.RequestVoteReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &proto.RequestVoteReply{}

	//fmt.Printf("我在给%d投票", args.CandidateId)
	reply.VoteGranted = false

	upToDate := false
	lastIndex := rf.getLastIndex()
	lastTerm := rf.log[lastIndex-rf.lastSnapIndex].Term

	if args.LastLogTerm > int32(lastTerm) || (args.LastLogTerm == int32(lastTerm) && args.LastLogIndex >= lastIndex) {
		upToDate = true
	}

	if args.Term > rf.term { //版本号大了，更新版本号，清空投票
		rf.term = args.Term
		rf.vote = -1
		rf.states = Follower
		rf.persist()
	}

	if args.Term < rf.term { //版本号小了，不投票
		reply.VoteGranted = false
		reply.Term = rf.term
		return reply, nil
	}

	if args.Term == rf.term &&
		(rf.vote == -1 || rf.vote == args.CandidateId) && upToDate { //版本号相同，未投票

		rf.vote = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
	}
	reply.Term = rf.term
	return reply, nil
}

// 向所有follwer发送心跳
func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peer int32) {
			rf.mu.Lock()
			next := rf.nextIndex[peer]
			prevIndex := next - 1

			var prevTerm int32

			if prevIndex < rf.lastSnapIndex {
				rf.mu.Unlock()
				rf.sendInstallSnapshot(peer)
				return
			}
			if prevIndex >= 0 {
				prevTerm = int32(rf.log[prevIndex-rf.lastSnapIndex].Term)
			}

			if next <= rf.lastSnapIndex { //日志同步落后于最后截断的快照，发送一整个快照过去
				rf.sendInstallSnapshot(peer)
				rf.mu.Unlock()
				return //发快照就不发心跳
			}

			entries := make([]types.LogEntry, len(rf.log[next-rf.lastSnapIndex:]))
			copy(entries, rf.log[next-rf.lastSnapIndex:])

			args := &HeartbeatArgs{
				LeaderId:          int32(rf.me),
				LeaderTerm:        rf.term,
				Entries:           entries,
				PreLogIndex:       prevIndex,
				PreLogTerm:        prevTerm,
				LeaderCommitIndex: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := &HeartbeatReply{}
			ok := rf.SendAppendEntries(peer, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.states != Leader || args.LeaderTerm != rf.term {
				return
			}

			if reply.Success {
				rf.lastHeartbeat = time.Now()
				if len(args.Entries) > 0 {

					rf.nextIndex[peer] = int32(int(args.PreLogIndex) + len(args.Entries) + 1)
					//成功对齐日志，记录成功数
					rf.matchIndex[peer] = int32(int(args.PreLogIndex) + len(args.Entries))
				} else {
					rf.nextIndex[peer] = args.PreLogIndex + 1
				}
				for N := rf.getLastIndex(); N > rf.commitIndex; N-- {
					if N <= rf.lastSnapIndex {
						continue
					}
					count := 1
					for j := range rf.peers {
						if j != rf.me && rf.matchIndex[j] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 && rf.log[N-rf.lastSnapIndex].Term == int32(rf.term) {
						rf.commitIndex = N
						break
					}
				}
			} else {

				rf.nextIndex[peer] = reply.ConflictIndex
				if rf.nextIndex[peer] > rf.getLastIndex()+1 {
					rf.nextIndex[peer] = rf.getLastIndex() + 1
				}

			}

			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.states = Follower
				rf.vote = -1
				rf.persist()
			}
		}(int32(i))
	}
}

func (rf *Raft) AppendEntries(ctx context.Context, args *proto.HeartbeatArgs) (*proto.HeartbeatReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &proto.HeartbeatReply{}
	if args.LeaderTerm < rf.term { //leader任期落后
		reply.Success = false
		reply.Term = rf.term
		reply.ConflictIndex = int32(len(rf.log))
		return reply, nil
	}

	rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)

	if args.LeaderTerm > rf.term { //自己落后，更新任期
		rf.term = args.LeaderTerm
		rf.vote = -1
		rf.persist()
	}
	rf.states = Follower
	rf.nowLeader = int64(args.LeaderId)

	if args.PreLogIndex > rf.getLastIndex() {
		reply.Success = false
		reply.Term = rf.term
		reply.ConflictIndex = rf.getLastIndex()
		return reply, nil
	}

	if args.PreLogIndex < rf.lastSnapIndex {
		reply.Success = false
		reply.ConflictIndex = rf.lastSnapIndex + 1
		reply.Term = rf.term
		return reply, nil
	}

	if args.PreLogIndex >= 0 && rf.log[args.PreLogIndex-rf.lastSnapIndex].Term != int32(args.PreLogTerm) { //发生冲突，找到冲突位置
		reply.Term = rf.term
		reply.Success = false
		index := args.PreLogIndex

		conflictTerm := rf.log[args.PreLogIndex-rf.lastSnapIndex].Term
		for index >= 0 && rf.log[index-rf.lastSnapIndex].Term == conflictTerm {
			index--
		}
		reply.ConflictIndex = index + 1
		return reply, nil
	}

	if args.LeaderCommitIndex > rf.commitIndex { //leader比自己先提交
		rf.commitIndex = min(args.LeaderCommitIndex, rf.getLastIndex())
	}

	index := args.PreLogIndex - rf.lastSnapIndex + 1
	incoming := make([]types.LogEntry, len(args.Entries))

	for i, e := range args.Entries {
		incoming[i] = types.LogEntry{
			Term:    int32(e.Term),
			Command: e.Command,
		}
	}
	for i, entry := range incoming {
		if int(index)+i < len(rf.log) {
			if rf.log[int(index)+i].Term != int32(entry.Term) { //发生冲突

				rf.log = rf.log[:int(index)+i] //从当前开始覆盖后面所有
				rf.log = append(rf.log, incoming[i:]...)
				rf.persist()
				break
			}
		} else {
			rf.log = append(rf.log, incoming[i:]...)
			rf.persist()
			break
		}
	}

	if len(args.Entries) > 0 {
		newLen := int(args.PreLogIndex) + len(args.Entries) + 1
		if newLen < len(rf.log) {
			rf.log = rf.log[:newLen]
		}
	}

	rf.persist()
	reply.Success = true
	reply.Term = rf.term
	return reply, nil
}

func (rf *Raft) SendAppendEntries(server int32, args *HeartbeatArgs, reply *HeartbeatReply) bool {

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	entries := make([]*proto.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &proto.LogEntry{
			Term:    int64(e.Term),
			Command: e.Command,
		}
	}

	res, err := rf.clients[server].AppendEntries(ctx, &proto.HeartbeatArgs{
		LeaderId:          int32(args.LeaderId),
		LeaderTerm:        int32(args.LeaderTerm),
		PreLogIndex:       int32(args.PreLogIndex),
		PreLogTerm:        int32(args.PreLogTerm),
		Entries:           entries,
		LeaderCommitIndex: int32(args.LeaderCommitIndex),
	})

	if err != nil {
		return false
	}

	reply.Success = res.Success
	reply.Term = int32(res.Term)
	reply.ConflictIndex = int32(res.ConflictIndex)

	return true
}
func (rf *Raft) sendInstallSnapshot(peer int32) {

	rf.mu.Lock()
	data := make([]byte, len(rf.snap))
	copy(data, rf.snap)

	args := &proto.InstallSnapshotArgs{
		Term:          int32(rf.term),
		LeaderId:      int32(rf.me),
		LastSnapIndex: int32(rf.lastSnapIndex),
		LastSnapTerm:  int32(rf.lastSnapTerm),
		Data:          data,
	}
	rf.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	res, err := rf.clients[peer].InstallSnapshot(ctx, args)
	if err != nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if int32(res.Term) > rf.term {
		rf.term = int32(res.Term)
		rf.states = Follower
		rf.persist()
		return
	}

	rf.nextIndex[peer] = rf.lastSnapIndex + 1
	rf.matchIndex[peer] = rf.lastSnapIndex
}

func (rf *Raft) InstallSnapshot(ctx context.Context, args *proto.InstallSnapshotArgs) (*proto.InstallSnapshotReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &proto.InstallSnapshotReply{}
	if args.Term < rf.term {
		reply.Term = rf.term
		return reply, nil
	}

	rf.states = Follower
	rf.term = args.Term
	rf.persist()

	if args.LastSnapIndex <= rf.lastSnapIndex {
		return reply, nil
	}

	oldSnapIndex := rf.lastSnapIndex

	if args.LastSnapIndex < rf.getLastIndex() {
		rf.log = rf.log[args.LastSnapIndex-oldSnapIndex:]
	} else { //丢弃旧log
		rf.log = []types.LogEntry{{Term: args.LastSnapTerm}}
	}

	rf.lastSnapIndex = args.LastSnapIndex
	rf.lastSnapTerm = args.LastSnapTerm

	rf.commitIndex = max(rf.commitIndex, rf.lastSnapIndex)
	rf.lastApply = max(rf.lastApply, rf.lastSnapIndex)

	rf.snap = make([]byte, len(args.Data))
	copy(rf.snap, args.Data)

	rf.persister.Save(rf.encodeState(), rf.snap)

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastSnapIndex,
			SnapshotTerm:  args.LastSnapTerm,
		}
	}()
	return reply, nil
}

func (rf *Raft) Start(data []byte) (int32, int32, bool, int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.term
	index := rf.getLastIndex() + 1 //日志编号
	isleader := true

	if rf.states != Leader {
		isleader = false
		return -1, term, isleader, rf.nowLeader
	} //不是leader不复制

	newcomm := types.LogEntry{
		Index:   index,
		Term:    rf.term,
		Command: data,
	}
	rf.nowLeader = int64(rf.me)
	rf.log = append(rf.log, newcomm)

	rf.wal.Append(wal.WalEntry{
		Index:   int64(newcomm.Index),
		Term:    int64(newcomm.Term),
		Command: newcomm.Command,
	})
	//	rf.commitIndex++
	rf.persist()
	go rf.broadcastAppendEntries()

	return int32(index), term, isleader, rf.nowLeader
}

// 无限循环选举，心跳发送
func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		states := rf.states
		rf.mu.Unlock()

		switch states {

		case Follower, Candidate: //follower和Candidate
			select {
			case <-rf.overElectiontime.C: //到达选举时间，给select一个信号使它触发
				//	fmt.Println("I am candidate:", rf.me, "term:", rf.term)
				rf.mu.Lock() //锁

				rf.states = Candidate
				rf.term++
				rf.vote = int32(rf.me) //为自己投一票

				term := rf.term
				me := rf.me

				rf.persist()

				// 重置选举超时(确保时间现在不是正在触发/刚触发状态，再重置选举超时时间)
				if !rf.overElectiontime.Stop() {
					select {
					case <-rf.overElectiontime.C:
					default:
					}
				}
				rf.overElectiontime.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)

				votes := 1 //该选举者获取的总票数
				lastIndex := rf.getLastIndex()
				lastterm := rf.log[lastIndex-rf.lastSnapIndex].Term
				rf.mu.Unlock()

				for i := range rf.peers {
					if i == me {
						continue
					}

					go func(server int32) {

						args := &RequestVoteArgs{
							Term:         term,
							CandidateId:  int32(me),
							LastLogIndex: lastIndex,
							LastLogTerm:  int32(lastterm),
						}
						reply := &RequestVoteReply{}

						ok := rf.sendRequestVote(server, args, reply)
						if !ok {
							return
						}

						rf.mu.Lock()

						// 过滤旧term
						if rf.states != Candidate || rf.term != term {
							rf.mu.Unlock()
							return
						}

						//发现更高term
						if reply.Term > rf.term {
							rf.term = reply.Term
							rf.states = Follower
							rf.vote = -1
							rf.persist()
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						if reply.IsVote == 1 {

							rf.mu.Lock()
							votes++

							if votes > len(rf.peers)/2 && rf.states == Candidate {

								rf.states = Leader
								rf.heartbeat.Reset(0)
								//		fmt.Println("I am leader:", rf.me, "term:", rf.term)
								// 当选为leader之后立刻发一次心跳告诉所有人

								for j := range rf.peers {
									rf.nextIndex[j] = rf.getLastIndex() + 1 //立刻更新对齐数
								}
							}
							rf.mu.Unlock()
						}
					}(int32(i))
				}
			default:
				time.Sleep(10 * time.Millisecond)
			}
		case Leader: //leader发送心跳

			select {
			case <-rf.heartbeat.C:
				rf.broadcastAppendEntries() //心跳
				rf.heartbeat.Reset(50 * time.Millisecond)
			default:
				time.Sleep(10 * time.Millisecond)
			}

		}
	}
}

// 检查是否有新日志可以提交，更新已执行的日志
func (rf *Raft) ApplyLoop() {
	for {

		rf.mu.Lock()
		if rf.lastApply < rf.commitIndex { //提交从commitindex到lastapply这个区间的日志
			rf.lastApply++
			//fmt.Println("我要提交日志")
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: int64(rf.lastApply),
				Command:      rf.log[rf.lastApply].Command,
			}

			rf.mu.Unlock()
			rf.applyCh <- msg //将要执行的日志传给上层kvserver
		} else { //没有新日志要提交
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func MakeRaft(applyCh chan ApplyMsg, peers []string, me int32, persister *persister.Persister) *Raft {
	rf := &Raft{}
	rf.wal = wal.NewWal("../download")
	if rf.wal.Exists() {
		rf.recoverFromWAL()
	}
	rf.me = int(me)  //暂时只有一个节点
	rf.peers = peers //暂时只有一个节点
	rf.term = 0
	rf.states = Follower
	rf.vote = -1
	rf.persister = persister
	rf.lastSnapIndex = 0
	rf.lastSnapTerm = 0
	rf.nextIndex = make([]int32, len(peers))
	rf.commitIndex = 0   //刚开始没有待提交的日志
	rf.lastApply = 0     //刚开始没有已经执行的日志
	rf.applyCh = applyCh //与上层kvserver联系的管道
	rf.snap = persister.ReadSnapshot()

	rf.matchIndex = make([]int32, len(peers))
	rf.overElectiontime = time.NewTimer(
		time.Duration(300+rand.Intn(300)) * time.Millisecond,
	) //随机生成选举超时
	for _, addr := range peers {
		conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		rf.clients = append(rf.clients, proto.NewRaftClient(conn))
	}
	rf.heartbeat = time.NewTimer(50 * time.Millisecond) //固定心跳发送时间

	rf.log = []types.LogEntry{{}} //dummy节点，log的index从1开始

	go rf.ApplyLoop() //循环发送要执行的日志给kvserver
	go rf.ticker()
	return rf

}
