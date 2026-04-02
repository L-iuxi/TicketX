package raft

import (
	"sync"
	"time"
)

// 传输消息的结构体
type ApplyMsg struct {
	CommandIndex int
	Command      interface{} //不关心类型
	CommandValid bool
}

// 定义server状态
type State string

const (
	Leader    State = "Leader"
	Candidate State = "Candidate"
	Follower  State = "Follower"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft结构体
type Raft struct {
	mu     sync.Mutex
	me     int        //当前服务器在peer的下标
	peers  []int      //存有所有服务器的组
	states State      //状态
	term   int        //当前任期号
	log    []LogEntry //日志

	commitIndex int           //等待提交的最新日志编号
	lastApply   int           //上次执行的最后一条日志编号
	applyCh     chan ApplyMsg //与kvserver沟通的渠道
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.states == Leader { //只有leader可以操作log
		addlog := LogEntry{
			Command: command,
			Term:    rf.term,
		}
		rf.log = append(rf.log, addlog) //把传过来的命令加入日志

		index = len(rf.log) - 1 //log从1开始
		rf.commitIndex = index  //单机情况下，加入即提交，后面要修改

		return index, rf.term, true
	} else {
		return 0, 0, false
	}
}

// 检查是否有新日志可以提交，更新已执行的日志
func (rf *Raft) ApplyLoop() {
	for {
		rf.mu.Lock()
		if rf.lastApply < rf.commitIndex { //提交从commitindex到lastapply这个区间的日志
			rf.lastApply++

			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApply,
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

func MakeRaft(applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.me = 0                 //暂时只有一个节点
	rf.peers = make([]int, 0) //暂时只有一个节点
	rf.term = 0
	rf.states = Leader   //单节点只有leader，后面多节点再把这个变成follwer
	rf.commitIndex = 0   //刚开始没有待提交的日志
	rf.lastApply = 0     //刚开始没有已经执行的日志
	rf.applyCh = applyCh //与上层kvserver联系的管道

	rf.log = []LogEntry{{}} //dummy节点，log的index从1开始

	go rf.ApplyLoop() //循环发送要执行的日志给kvserver
	return rf

}
