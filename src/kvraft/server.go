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
	Ch chan Op
}

type OpState string

const (
	StatePending = "Pending"
	StateComplete = "Complete"
	StateAbort = "Abort"
)

type OpResponse struct {
	Result string
	State OpState
	Ch chan OpResponse
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
	
	rpcMap map[int]OpResponse
	rpcMapMu *sync.Mutex

	Leader bool
}

func (kv *RaftKV) TryCommit(id int, key string, val string, op OPCode) (wrongLeader bool, result string) {
	
	wrongLeader = false
	
	kv.rpcMapMu.Lock()
	response, ok := kv.rpcMap[id]
	kv.rpcMapMu.Unlock()
	
	if(ok) {
		fmt.Printf("server, return cached result ID %v, [%v]=[%v]\n", id, key, response.Result)
		result = response.Result
		return
	}
	
	command := Op{}
	command.Key = key
	command.Val = val
	command.Code = op
	command.ID = id
	
	_, _, isLeader := kv.rf.Start(command)
	// fmt.Printf("## server, %v isleader %v tryCommit [%v]\n", kv.me, isLeader, key)
	if(isLeader == false) {
		wrongLeader = true
		kv.Leader = false
		// fmt.Printf("server, %v wrong leader\n", kv.me)
		return
	}
	
	kv.Leader = true
	opRes := OpResponse{}
	opRes.Ch = make(chan OpResponse)
	opRes.State = StatePending
	
	kv.rpcMapMu.Lock()
	kv.rpcMap[command.ID] = opRes
	kv.rpcMapMu.Unlock()
	
	opRes =<- opRes.Ch
	result = opRes.Result
	
	if(opRes.State != StateComplete){
		fmt.Printf("Op [%v] aborted [%v]\n", command.ID, command.Key)
		delete(kv.rpcMap, command.ID)
		wrongLeader = true
		return	
	}
	
	kv.rpcMapMu.Lock()
	kv.rpcMap[command.ID] = opRes
	kv.rpcMapMu.Unlock()
	
	return	
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = Err_Success	
	reply.WrongLeader = "false"
	reply.ServerId = kv.me
	// fmt.Printf(">> server ID [%v] %v Get key [%v] \n", args.ID, kv.me, args.Key)
	
	wrongLeader, result := kv.TryCommit(args.ID, args.Key, "", Get)
	
	if(wrongLeader) {
		reply.WrongLeader = "true"
		kv.Leader = false
		fmt.Printf("#### server: %v wrong leader\n", kv.me)
		return
	}
	
	kv.Leader = true
	// fmt.Printf(">> server ID [%v] %v Get key [%v] = %v\n", args.ID, kv.me, args.Key, result)
	reply.Value = result
	
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	
	reply.Err = Err_Success
	reply.WrongLeader = "false"
	reply.ServerId = kv.me
	
	wrongLeader, _ := kv.TryCommit(args.ID, args.Key, args.Value, args.Op)
	
	if(wrongLeader) {
		reply.WrongLeader = "true"
		// fmt.Printf("#### server: %v wrong leader\n", kv.me)
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

func (kv *RaftKV) ApplyToStateMachine(applyCmd Op) (result string) {
	
	log := ""
	result = ""
	
	kv.statesMu.Lock()
	
	if(applyCmd.Code == Put) {
		kv.states[applyCmd.Key] = applyCmd.Val
		
		log = fmt.Sprintf("=== server %v, apply [#%v] Put [%v] = [%v]\n", 
				kv.me, applyCmd.ID, applyCmd.Key, kv.states[applyCmd.Key])
		
	} else if(applyCmd.Code == Append){
		kv.states[applyCmd.Key] += applyCmd.Val
		
		log = fmt.Sprintf("=== server %v, apply [#%v] Append [%v] = [%v]\n", 
			kv.me, applyCmd.ID, applyCmd.Key, kv.states[applyCmd.Key])
			
	} else {
		result = kv.states[applyCmd.Key]
		
		log = fmt.Sprintf("=== server %v, apply [#%v] Get [%v] = [%v]\n", 
			kv.me, applyCmd.ID, applyCmd.Key, result)
	}
	
	kv.statesMu.Unlock()
	
	if(kv.Leader) {
		fmt.Printf(log)
	}
	
	return
}

func (kv *RaftKV) HandleApplyMsg() {
	
	for {
		
		applyMsg :=<- kv.applyCh
		
		applyCmd := applyMsg.Command.(Op)
	
		result := kv.ApplyToStateMachine(applyCmd)
		
		kv.rpcMapMu.Lock()
		opRes := kv.rpcMap[applyCmd.ID]
		kv.rpcMapMu.Unlock()
		
		opRes.Result = result
		opRes.State = StateComplete
		
		opRes.Ch <- opRes
	}
	
}

func (kv *RaftKV) HandleLostLeader() {
	for {
			
		<-kv.rf.LostLeaderCh

		// if raft lost the leader, abort all the pending requests so client can retry
		fmt.Printf("!! server %v lost leader\n", kv.me)
			
		kv.rpcMapMu.Lock()
		for k, v := range kv.rpcMap {
			
			if(v.State == StatePending) {
				fmt.Printf("    server: abort request %v\n", k)
				v.State = StateAbort
				v.Ch <- v
				kv.rpcMap[k] = v
			}
		}
		kv.rpcMapMu.Unlock()
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
	
	kv.rpcMap = make(map[int]OpResponse)
	kv.rpcMapMu = &sync.Mutex{}
	
	go kv.HandleApplyMsg()
	go kv.HandleLostLeader()
	
	fmt.Printf("===== StartKVServer %v =====\n", me)
	
	return kv
}
