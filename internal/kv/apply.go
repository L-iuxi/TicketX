package kv

import "fmt"

// 接受管道来到msg并执行
func (kv *KvServer) applyLoop() {

	for msg := range kv.applyCh {

		if msg.CommandValid {
			op := msg.Command.(Op)

			kv.mu.Lock()
			switch op.Type {
			case Put:
				last, ok := kv.lastRequest[op.ClientId]

				if ok && op.RequestId <= last { //重复请求不重复执行只返回
				} else { //未重复请求执行并记录
					kv.kv[op.Key] = op.Value
					kv.lastRequest[op.ClientId] = op.RequestId
					fmt.Printf("健 %s 值 %s", op.Key, op.Value)

				}
				//返回结果给put
				if ch, ok := kv.waitCh[msg.CommandIndex]; ok {
					ch <- result{Success: true}
					delete(kv.waitCh, msg.CommandIndex)
				}
			case Get: //不在乎重复请求
				if ch, ok := kv.getCh[msg.CommandIndex]; ok {
					ch <- result{Value: kv.kv[op.Key], Success: true}
					delete(kv.getCh, msg.CommandIndex)
				}
			}
			kv.mu.Unlock()
		}

	}
}
