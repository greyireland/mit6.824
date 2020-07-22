package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OpPut    = "OpPut"
	OpGet    = "OpGet"
	OpAppend = "OpAppend"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind  string
	Key   string
	Value string
	Id    int64
	ReqId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db     map[string]string
	ack    map[int64]int
	result map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{
		Kind:  OpGet,
		Key:   args.Key,
		Id:    args.Id,
		ReqId: args.ReqId,
	}
	ok := kv.appendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.ack[args.Id] = args.ReqId
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{
		Kind:  args.Op,
		Key:   args.Key,
		Value: args.Value,
		Id:    args.Id,
		ReqId: args.ReqId,
	}
	ok := kv.appendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}
func (kv *KVServer) appendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		return op == entry
	case <-time.After(time.Second):
		return false
	}
}
func (kv *KVServer) Apply(op Op) {
	switch op.Kind {
	case OpPut:
		kv.db[op.Key] = op.Value
	case OpAppend:
		kv.db[op.Key] += op.Value
	}
	// 重复应用时去重检查
	kv.ack[op.Id] = op.ReqId
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.db = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.result = make(map[int]chan Op)
	go kv.applyLog()
	return kv
}

func (kv *KVServer) applyLog() {
	for {
		msg := <-kv.applyCh
		if msg.UseSnapshot {
			var lastIncludedIndex int
			var lastIncludedTerm int
			r := bytes.NewBuffer(msg.Snapshot)
			d := gob.NewDecoder(r)

			d.Decode(&lastIncludedIndex)
			d.Decode(&lastIncludedTerm)
			db := make(map[string]string)
			ack := make(map[int64]int)
			d.Decode(&db)
			d.Decode(&ack)
			// 修改加锁
			kv.mu.Lock()
			kv.db = db
			kv.ack = ack
			kv.mu.Unlock()
		} else {
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}
			kv.mu.Lock()
			if !kv.isDupApply(op.Id, op.ReqId) {
				kv.Apply(op) // 写入DB
			}
			ch, ok := kv.result[msg.CommandIndex]
			if ok {
				select {
				case <-kv.result[msg.CommandIndex]:
				default:
					DPrintf("result empty")
				}
				ch <- op
			} else {
				kv.result[msg.CommandIndex] = make(chan Op, 1)
			}
			// 超过限定大小进行Snapshot快照合并
			if kv.maxraftstate != -1 && kv.rf.GetPersistSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.db)
				e.Encode(kv.ack)
				data := w.Bytes()
				go kv.rf.StartSnapshot(data, msg.CommandIndex)
			}
			kv.mu.Unlock()

		}
	}
}
func (kv *KVServer) isDupApply(id int64, reqId int) bool {
	v, ok := kv.ack[id]
	if ok {
		return v >= reqId
	}
	return false
}
