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
	WrongLeader bool
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
	
	currIndex int
}

func (kv *ShardKV) TryCommit(id int, key string, val string, op OPCode) (result OpResponse) {
	
	opRes := OpResponse{}
	opRes.WrongLeader = false
	opRes.Err = OK
	opRes.Result = ""
	
	kv.rpcMapMu.Lock()
	response, ok := kv.rpcMap[id]
	kv.rpcMapMu.Unlock()
	
	if(ok) {
		opRes.Result = response.Result
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
		result.WrongLeader = true
		kv.Leader = false
		return
	}
	
	kv.Leader = true

	opRes.Ch = make(chan OpResponse)
	opRes.State = StatePending
	
	kv.rpcMapMu.Lock()
	kv.rpcMap[command.ID] = opRes
	kv.rpcMapMu.Unlock()
	
	opRes =<- opRes.Ch
	result.Err = opRes.Err
	result.State = opRes.State
	result.Result = opRes.Result
	
	if(opRes.State != StateComplete){
		fmt.Printf("Op [%v] aborted [%v]\n", command.ID, command.Key)
		delete(kv.rpcMap, command.ID)
		opRes.WrongLeader = true
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
	err = OK
				
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
		// fmt.Printf(log)
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
		time.Sleep(500 * time.Millisecond)
	}
}

func (kv *ShardKV) NewID() int {
	
	kv.currIndex ++
	newId := kv.gid*1000*10 + kv.currIndex
	
	return newId
}

func (kv *ShardKV) MoveOutKey(key string, servers []string) {
	
	val,ok := kv.states[key]
	
	if(!ok) {
		return	
	}
	
	args := PutAppendArgs{}
	args.Key = key
	args.Value = val
	args.Op = OPPut
	args.ID = kv.NewID()
	
	// try each server for the shard.
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		
		reply := PutAppendReply{}
		ok = srv.Call("ShardKV.PutAppend", &args, &reply)
		
		if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
			return
		}
		
		if ok && (reply.Err == ErrWrongGroup) {
			break
		}
	}  
}

func (kv *ShardKV) CheckConfig() {
	
	if kv.config.Num == 0 {
		kv.config = kv.sm.Query(-1)
		return
	}
	
	newConfig := kv.sm.Query(-1)
	if newConfig.Num != kv.config.Num {
		
		for i := 0;i<len(newConfig.Shards);i++ {
			
			if(newConfig.Shards[i] == kv.gid &&
			   kv.config.Shards[i] != kv.gid) {
				 // move in
			} else if(newConfig.Shards[i] != kv.gid &&
			          kv.config.Shards[i] == kv.gid) {
				// move out
				newOwner := newConfig.Shards[i]
				servers, ok := newConfig.Groups[newOwner]
				if(!ok) {
					fmt.Printf("ERROR - server, newOwner not exists [%v]\n", newOwner)	
				} else {
					
					for key := range kv.states {
						
						if key2shard(key) == i {
							fmt.Printf("server, moving out key [%v] servers [%v]\n", key, servers)
							kv.MoveOutKey(key, servers)
						}
					}
				}
			}
			
		}
	}
	
	kv.config = newConfig
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = OK	
	reply.WrongLeader = false
	// fmt.Printf(">> server ID [%v] %v Get key [%v] \n", args.ID, kv.me, args.Key)
	
	kv.CheckConfig()
	
	shard := key2shard(args.Key)
	if(kv.config.Shards[shard] != kv.gid) {
		reply.Err = ErrWrongGroup
		return
	}
	
	opRes := kv.TryCommit(args.ID, args.Key, "", OPGet)
	
	if(opRes.WrongLeader) {
		reply.WrongLeader = true
		kv.Leader = false
		// fmt.Printf("#### server: %v wrong leader\n", kv.me)
		return
	}
	
	kv.Leader = true
	// fmt.Printf(">> server ID [%v] %v Get key [%v] = %v\n", args.ID, kv.me, args.Key, result)
	reply.Value = opRes.Result
	// reply.Err = opRes.Err
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK
	reply.WrongLeader = false
	
	// fmt.Printf("server, PutAppend %v, args %v\n", kv.me, args)
	
	kv.CheckConfig()
	
	shard := key2shard(args.Key)
	if(kv.config.Shards[shard] != kv.gid) {
		reply.Err = ErrWrongGroup
		return
	}
	
	opRes := kv.TryCommit(args.ID, args.Key, args.Value, args.Op)
	
	if(opRes.WrongLeader) {
		reply.WrongLeader = true
		// fmt.Printf("#### server: %v wrong leader\n", kv.me)
		return
	}
	
	// reply.Err = opRes.Err
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
	// go kv.PollConfig()
	
	fmt.Printf("===== StartKVServer %v =====\n", me)
	
	return kv
}
