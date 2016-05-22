package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID int
	Code OPCode
	Key string
	Val string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	states  map[string]string
	statesMu *sync.Mutex
	
	currID int
	currIDMu *sync.Mutex
	
	rpcMap map[int]chan string
	rpcMapMu *sync.Mutex
}

func (kv *RaftKV) TryCommit(key string, val string, op OPCode) (wrongLeader bool, result string) {
	
	wrongLeader = false
	
	command := Op{}
	command.Key = key
	command.Val = val
	command.Code = op
	
	kv.currIDMu.Lock()
	kv.currID ++
	command.ID = kv.currID
	kv.currIDMu.Unlock()
	_, _, isLeader := kv.rf.Start(command)
	if(!isLeader) {
		wrongLeader = true
		// fmt.Printf("%v wrong leader\n", kv.me)
		return
	}
	
	ch := make(chan string)
	
	kv.rpcMapMu.Lock()
	kv.rpcMap[command.ID] = ch
	kv.rpcMapMu.Unlock()
	
	result =<- ch
	
	return	
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = Err_Success	
	reply.WrongLeader = "false"
	
	wrongLeader, result := kv.TryCommit(args.Key, "", Get)
	
	if(wrongLeader) {
		reply.WrongLeader = "true"
		return
	}
	
	fmt.Printf(">> server %v Get key [%v] = %v\n", kv.me, args.Key, result)
	reply.Value = result
	
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	
	wrongLeader, _ := kv.TryCommit(args.Key, args.Value, args.Op)
	
	if(wrongLeader) {
		reply.WrongLeader = "true"
		return
	}
	
	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) HandleApplyMsg() {
	
	for {
		applyCmd := Op{}
		
		applyMsg :=<- kv.applyCh
		
		applyCmd = applyMsg.Command.(Op)
	
		result := ""
		log := ""
		
		kv.statesMu.Lock()
	
		if(applyCmd.Code == Put) {
			kv.states[applyCmd.Key] = applyCmd.Val
			
			log = fmt.Sprintf("=== server %v Put [%v] = [%v]\n", 
					kv.me, applyCmd.Key, kv.states[applyCmd.Key])
			
		} else if(applyCmd.Code == Append){
			kv.states[applyCmd.Key] += applyCmd.Val
			
			log = fmt.Sprintf("=== server %v Append [%v] = [%v]\n", 
				kv.me, applyCmd.Key, kv.states[applyCmd.Key])
				
		} else {
			result = kv.states[applyCmd.Key]
			
			log = fmt.Sprintf("=== server %v Get [%v] = [%v]\n", 
				kv.me, applyCmd.Key, result)
		}
		
		fmt.Printf(log)
		
		kv.statesMu.Unlock()
	
		kv.rpcMapMu.Lock()
		ch := kv.rpcMap[applyCmd.ID]
		kv.rpcMapMu.Unlock()
		
		ch <- result
	}
	
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.states = make(map[string]string)
	kv.statesMu = &sync.Mutex{}
	
	kv.currID = 0
	kv.currIDMu = &sync.Mutex{}
	
	kv.rpcMap = make(map[int]chan string)
	kv.rpcMapMu = &sync.Mutex{}
	
	go kv.HandleApplyMsg()
	
	fmt.Println()
	
	return kv
}
