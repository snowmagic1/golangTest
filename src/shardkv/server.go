package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "fmt"
import "time"

import "shardmaster"

type OPCode string

const (
	OPGet = "Get"
	OPPut = "Put"
	OPAppend = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID int
	Code OPCode
	Key string
	Val string
}

func (op Op) String() string {
	return fmt.Sprintf("Index [%v] [%v] [%v]=[%v]", 
		op.ID, op.Code,op.Key,op.Val)
}

type OpState string

const (
	StatePending = "Pending"
	StateComplete = "Complete"
	StateAbort = "Abort"
)

type OpResponse struct {
	Result string
	Err Err
	State OpState
	Ch chan OpResponse
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	states  map[string]string
	statesMu *sync.Mutex
	
	rpcMap map[int]OpResponse
	rpcMapMu *sync.Mutex

	Leader bool
	
	sm       *shardmaster.Clerk
	config   shardmaster.Config
}

func (kv *ShardKV) TryCommit(id int, key string, val string, op OPCode) (wrongLeader bool, result string) {
	
	wrongLeader = false
	
	kv.rpcMapMu.Lock()
	response, ok := kv.rpcMap[id]
	kv.rpcMapMu.Unlock()
	
	if(ok) {
		result = response.Result
		fmt.Printf("server, return cached result ID %v, [%v] [%v] [%v]\n", id, key, op, val)
		return
	}
	
	command := Op{}
	command.Key = key
	command.Val = val
	command.Code = op
	command.ID = id
	
	_, _, isLeader := kv.rf.Start(command)
	if(isLeader == false) {
		wrongLeader = true
		kv.Leader = false
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

func (kv *ShardKV) ApplyToStateMachine(applyOp Op) (result string, err Err) {
	
	log := fmt.Sprintf("server %v, ApplyToStateMachine [%v]\n", 
				kv.me, applyOp)
	
	result = ""
	err = ""
				
	kv.statesMu.Lock()
	
	switch applyOp.Code {
		case OPPut:
			kv.states[applyOp.Key] = applyOp.Val
			
		case OPAppend:
			if _, ok := kv.states[applyOp.Key]; !ok {
				log += fmt.Sprintf("Error: server, Key [%v] doesn't exit\n", applyOp.Key)
				err = ErrNoKey
				break	
			}
			
			kv.states[applyOp.Key] += applyOp.Val
			
		case OPGet:
			result = kv.states[applyOp.Key]
			if _, ok := kv.states[applyOp.Key]; !ok {
				log += fmt.Sprintf("Error: server, Key [%v] doesn't exit\n", applyOp.Key)
				err = ErrNoKey
				break	
			}
			
		default:
			log += fmt.Sprintf("server, unknown OP code [%v]\n", applyOp.Code)
	}
	
	kv.statesMu.Unlock()
	
	if(kv.Leader) {
		fmt.Printf(log)
	}
	
	return
}

func (kv *ShardKV) HandleApplyMsg() {
	
	for {
		
		applyMsg :=<- kv.applyCh
		
		applyOp := applyMsg.Command.(Op)
	
		result, err := kv.ApplyToStateMachine(applyOp)
		
		kv.rpcMapMu.Lock()
		opRes, ok := kv.rpcMap[applyOp.ID]
		kv.rpcMapMu.Unlock()
		
		opRes.Result = result
		opRes.State = StateComplete
		opRes.Err = err
		
		if(ok) {
			
			// Leader who handles client's request	
			opRes.Ch <- opRes
		} else {
			kv.rpcMap[applyOp.ID] = opRes
		}
	}
	
}

func (kv *ShardKV) HandleLostLeader() {
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

func (kv *ShardKV) PollConfig() {
	for {
		kv.config = kv.sm.Query(-1)
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = ""	
	reply.WrongLeader = false
	// fmt.Printf(">> server ID [%v] %v Get key [%v] \n", args.ID, kv.me, args.Key)
	
	shard := key2shard(args.Key)
	if(kv.config.Shards[shard] != kv.gid) {
		reply.Err = ErrWrongGroup
		return
	}
	
	wrongLeader, result := kv.TryCommit(args.ID, args.Key, "", OPGet)
	
	if(wrongLeader) {
		reply.WrongLeader = true
		kv.Leader = false
		// fmt.Printf("#### server: %v wrong leader\n", kv.me)
		return
	}
	
	kv.Leader = true
	// fmt.Printf(">> server ID [%v] %v Get key [%v] = %v\n", args.ID, kv.me, args.Key, result)
	reply.Value = result
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = ""
	reply.WrongLeader = false
	
	shard := key2shard(args.Key)
	if(kv.config.Shards[shard] != kv.gid) {
		reply.Err = ErrWrongGroup
		return
	}
	
	wrongLeader, _ := kv.TryCommit(args.ID, args.Key, args.Value, args.Op)
	
	if(wrongLeader) {
		reply.WrongLeader = true
		// fmt.Printf("#### server: %v wrong leader\n", kv.me)
		return
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.states = make(map[string]string)
	kv.statesMu = &sync.Mutex{}
	
	kv.rpcMap = make(map[int]OpResponse)
	kv.rpcMapMu = &sync.Mutex{}
	
	kv.sm = shardmaster.MakeClerk(masters)
	kv.make_end = make_end
	
	go kv.HandleApplyMsg()
	go kv.HandleLostLeader()
	go kv.PollConfig()
	
	fmt.Printf("===== StartKVServer %v =====\n", me)
	
	return kv
}
