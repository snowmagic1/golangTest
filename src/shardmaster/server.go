package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "fmt"

type Op struct {
	ID int
	Code OPCode
	
	// Join
	Servers map[int][]string
	
	// Leave
	GIDs []int
	
	// Move
	Shard int
	GID   int
	
	// Query
	Num int
}

func (op Op) String() string {
	switch op.Code {
		case Join:
			return fmt.Sprintf("Join: [%v]", op.Servers)
		case Leave:
			return fmt.Sprintf("Leave: [%v]", op.GIDs)
		case Move:
			return fmt.Sprintf("Move: Shard [%v] GID [%v]", op.Shard, op.GID)
		case Get:
			return fmt.Sprintf("Query: Num [%v]", op.Num)
		default:
			return "Unkown OP type"
	}
}

type OpState string

const (
	StatePending = "Pending"
	StateComplete = "Complete"
	StateAbort = "Abort"
)

type OpResponse struct {
	Reply interface{}
	State OpState
	Ch chan OpResponse
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	rpcMap map[int]OpResponse
	rpcMapMu *sync.Mutex

	Leader bool
	
	currConfig int
	configs []Config // indexed by config num
	configsMu *sync.Mutex
}

func (sm *ShardMaster) TryCommit(id int, args interface{}, op OPCode) (wrongLeader bool, reply interface{}) {
	
	wrongLeader = false
	
	sm.rpcMapMu.Lock()
	response, ok := sm.rpcMap[id]
	sm.rpcMapMu.Unlock()
	
	if(ok) {
		reply = response.Reply
		fmt.Printf("server, return cached result ID %v, [%v] [%v]\n", id, args, op)
		return
	}
	
	command := Op{}
	command.Code = op
	command.ID = id
	
	switch v := args.(type) {
		case JoinArgs:
			command.Servers = args.(JoinArgs).Servers
			
		case LeaveArgs:
			command.GIDs = args.(LeaveArgs).GIDs
			
		case MoveArgs:
			command.GID = args.(MoveArgs).GID
			command.Shard = args.(MoveArgs).Shard
			
		case QueryArgs:
			command.Num = args.(QueryArgs).Num
			
		default:
			fmt.Printf("server, unknown args type %v\n", v)
			return	
	}
	
	_, _, isLeader := sm.rf.Start(command)
	// fmt.Printf("## server, %v isleader %v tryCommit [%v]\n", sm.me, isLeader, key)
	if(isLeader == false) {
		wrongLeader = true
		sm.Leader = false
		// fmt.Printf("server, %v wrong leader\n", sm.me)
		return
	}
	
	sm.Leader = true
	opRes := OpResponse{}
	opRes.Ch = make(chan OpResponse)
	opRes.State = StatePending
	
	sm.rpcMapMu.Lock()
	sm.rpcMap[command.ID] = opRes
	sm.rpcMapMu.Unlock()
	
	opRes =<- opRes.Ch
	reply = opRes.Reply
	// fmt.Printf("server, tryCommit reply [%v]\n", reply)
	
	if(opRes.State != StateComplete){
		fmt.Printf("Op [%v] aborted [%v]\n", command.ID, args)
		delete(sm.rpcMap, command.ID)
		wrongLeader = true
		return	
	}
	
	sm.rpcMapMu.Lock()
	sm.rpcMap[command.ID] = opRes
	sm.rpcMapMu.Unlock()
	
	return	
}

func (sm *ShardMaster) NewConfig(newShards [NShards]int, newGroups map[int][]string) {
	
	newConfig := Config{}
	sm.currConfig ++
	newConfig.Num = sm.currConfig	
	newConfig.Groups = newGroups
	newConfig.Shards = newShards
	
	// fmt.Printf("\nNewConfig [%v]\n", sm.configs)
	sm.configs = append(sm.configs, newConfig)
	// fmt.Printf("NewConfig %v configs [%v]\n\n", newConfig, sm.configs)
}

func (sm *ShardMaster) CloneGroups() map[int][]string {
	cloned := make(map[int][]string)	
	
	for k,v := range sm.configs[sm.currConfig].Groups {
		cloned[k] = v
	}
	
	return cloned
}

func (sm *ShardMaster) MinMaxLoadGroupID() (minLoadId, maxLoadId, delta int) {
	
	minLoadId = 0
	maxLoadId = 0
	
	currConfig := sm.configs[sm.currConfig]
	shards := currConfig.Shards
	groups := currConfig.Groups
	
	loadMap := make(map[int]int)
	for gid := range groups {
		loadMap[gid] = 0	
	} 
	
	for _, gid := range shards{
		if(gid != 0){
			loadMap[gid] ++
		}
	}
	
	minLoad := NShards + 1
	maxLoad := 0
	
	for gid, load := range loadMap {
		if(load <= minLoad) {
			minLoadId = gid
			minLoad = loadMap[gid]
		}
		
		if(load >= maxLoad){
			maxLoadId = gid
			maxLoad = loadMap[gid]
		}
	}
	
	delta = loadMap[maxLoadId] - loadMap[minLoadId]
	// fmt.Printf("  >> server, min [%v] max [%v] delta [%v] <<\n", minLoadId, maxLoadId, delta)
	return
}

func (sm *ShardMaster) BalanceShardsForJoin() {
	
	currConfig := sm.configs[sm.currConfig]
	if(len(currConfig.Groups) == 0) {
		fmt.Printf("ERROR, server no groupID\n")
		return	
	} 
	
	min, max, delta := sm.MinMaxLoadGroupID()
	
	for(delta > 1) {
		
		for i := 0;i<len(currConfig.Shards);i++{
			if(currConfig.Shards[i] == max){
				currConfig.Shards[i] = min
				break
			}
		}
		sm.configs[sm.currConfig] = currConfig
		min, max, delta = sm.MinMaxLoadGroupID()
	}
}

func (sm *ShardMaster) BalanceShardsForLeave() {
	
	currConfig := sm.configs[sm.currConfig]
	if(len(currConfig.Groups) == 0) {
		fmt.Printf("ERROR, server no groupID\n")
		return	
	} 
		
	for i := 0;i<len(currConfig.Shards);i++{
		if(currConfig.Shards[i] == 0){
			min, _, _ := sm.MinMaxLoadGroupID()
			currConfig.Shards[i] = min
		}
		sm.configs[sm.currConfig] = currConfig
	}
}

func (sm *ShardMaster) ApplyToStateMachine(applyOp Op) (reply interface{}) {
	
	// fmt.Printf("#### %v start ApplyToStateMachine %v ####\n", sm.me,applyOp)
	
	log := ""
	
	sm.configsMu.Lock()

	currConfig := sm.configs[sm.currConfig]
	currGroups := sm.CloneGroups()
	currShards := currConfig.Shards
			
	switch applyOp.Code{
		case Join:
			serversMap := applyOp.Servers
			joinReply := JoinReply{}
			
			for groupId, servers := range serversMap {
				
				_, ok := currGroups[groupId]
				
				if(ok) {
					log += fmt.Sprintf("ERROR - server, groupId [%v] already exists\n", groupId)
					continue
				}
				
				currGroups[groupId] = servers
			}
			
			sm.NewConfig(currShards, currGroups)
			if(len(sm.configs) == 2) {
				sm.BalanceShardsForLeave()
			} else {
				sm.BalanceShardsForJoin()	
			}
			
			joinReply.Err = Err_Success
			joinReply.WrongLeader = false
			
			reply = joinReply
			
			// log += fmt.Sprintf("server, curr %v configs [%v]\n", sm.currConfig, sm.configs)
			log += fmt.Sprintf("server, joins servers [%v] config [%v]\n", serversMap, sm.configs[sm.currConfig])
			
		case Leave:
	
			gids := applyOp.GIDs
			leaveReply := LeaveReply{}
			
			for _, gid := range gids {
				_, ok := currGroups[gid]
				
				if(!ok) {
					log += fmt.Sprintf("ERROR - server, groupId [%v] not exists\n", gid)
					continue
				}
				
				delete(currGroups, gid)
				
				for i:=0;i<len(currShards);i++ {
					if(currShards[i] == gid) {
						currShards[i] = 0
					}
				}
			}
			
			sm.NewConfig(currShards, currGroups)
			sm.BalanceShardsForLeave()
			
			leaveReply.Err = Err_Success
			leaveReply.WrongLeader = false
			reply = leaveReply
			
			log += fmt.Sprintf("server, leave gids %v\n", gids)
			
		case Move:
		
			shard := applyOp.Shard
			gid := applyOp.GID
			
			moveReply := MoveReply{}
			
			_, ok := currGroups[gid]
			if(!ok) {
				moveReply.Err = Err_InvalidParameter
				reply = moveReply
				log += fmt.Sprintf("ERROR - server, Move, groupId [%v] not exists\n", gid)
					
				break	
			}
			
			if(shard < 0 || shard >= NShards) {
				moveReply.Err = Err_InvalidParameter
				reply = moveReply
				log += fmt.Sprintf("ERROR - server, Move, invalid shard [%v]\n", shard)
					
				break
			}
			
			currShards[shard] = gid
			
			sm.NewConfig(currShards, currGroups)
			// sm.BalanceShardsForJoin()
						
			moveReply.Err = Err_Success
			moveReply.WrongLeader = false
			
			reply = moveReply
			
			log += fmt.Sprintf("server, Move shard [%v] to gid [%v]\n", shard, gid)
			
		case Get:
			num := applyOp.Num
			queryReply := QueryReply{}
			
			if(num < 0 || num >= len(sm.configs)) {
				queryReply.Config = sm.configs[sm.currConfig]	
			} else {
				queryReply.Config = sm.configs[num]
			}
			
			queryReply.Err = Err_Success
			queryReply.WrongLeader = false
			
			reply = queryReply
			
			log += fmt.Sprintf("server, Get config [%v]=[%v]\n", num, queryReply.Config)
			
		default:
			log += fmt.Sprintf("server, unknown OP code [%v]\n", applyOp.Code)
			
	}
	
	sm.configsMu.Unlock()
	
	if(sm.Leader) {
		fmt.Printf("  " + log)
	}
	
	// fmt.Printf("#### %v End ApplyToStateMachine %v ####\n", sm.me, applyOp)
	
	return
}

func (sm *ShardMaster) HandleApplyMsg() {
	
	for {
		
		applyMsg :=<- sm.applyCh
		// fmt.Printf("server %v, apply msg [%v]\n", sm.me, applyMsg)
		applyCmd := applyMsg.Command.(Op)
	
		reply := sm.ApplyToStateMachine(applyCmd)
		
		// fmt.Printf("server %v, set rpc map \n", sm.me)
		
		sm.rpcMapMu.Lock()
		opRes, ok := sm.rpcMap[applyCmd.ID]
		sm.rpcMapMu.Unlock()
		
		opRes.Reply = reply
		opRes.State = StateComplete
			
		if(ok) {
			
			// Leader who handles client's request	
			opRes.Ch <- opRes
		} else {
			sm.rpcMap[applyCmd.ID] = opRes
		}
	}
}

func (sm *ShardMaster) HandleLostLeader() {
	for {
			
		<-sm.rf.LostLeaderCh

		// if raft lost the leader, abort all the pending requests so client can retry
		fmt.Printf("!! server %v lost leader\n", sm.me)
			
		sm.rpcMapMu.Lock()
		for k, v := range sm.rpcMap {
			
			if(v.State == StatePending) {
				fmt.Printf("    server: abort request %v\n", k)
				v.State = StateAbort
				v.Ch <- v
				sm.rpcMap[k] = v
			}
		}
		sm.rpcMapMu.Unlock()
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	
	wrongLeader, r := sm.TryCommit(args.ID, *args, Join)
	
	if(wrongLeader) {
		reply.WrongLeader = true
		// fmt.Printf("#### server: %v wrong leader\n", sm.me)
		return
	}
	
	rr := r.(JoinReply)
	reply.Err = rr.Err
	reply.WrongLeader = rr.WrongLeader
	
	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	
	wrongLeader, r := sm.TryCommit(args.ID, *args, Leave)
	
	if(wrongLeader) {
		reply.WrongLeader = true
		// fmt.Printf("#### server: %v wrong leader\n", sm.me)
		return
	}
	
	rr := r.(LeaveReply)
	reply.Err = rr.Err
	reply.WrongLeader = rr.WrongLeader
	
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {

	wrongLeader, r := sm.TryCommit(args.ID, *args, Move)
	
	if(wrongLeader) {
		reply.WrongLeader = true
		// fmt.Printf("#### server: %v wrong leader\n", sm.me)
		return
	}
	
	rr := r.(MoveReply)
	reply.Err = rr.Err
	reply.WrongLeader = rr.WrongLeader
	
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {

	wrongLeader, r := sm.TryCommit(args.ID, *args, Get)
	
	if(wrongLeader) {
		reply.WrongLeader = true
		// fmt.Printf("#### server: %v wrong leader\n", sm.me)
		return
	}
	
	rr := r.(QueryReply)
	// fmt.Printf("server, Query reply [%v]\n", rr)
	reply.Config = rr.Config
	reply.Err = rr.Err
	reply.WrongLeader = rr.WrongLeader
	
	return
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	
	fmt.Printf("Start server %v\n", me)
	
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.rpcMap = make(map[int]OpResponse)
	sm.rpcMapMu = &sync.Mutex{}
	
	sm.Leader = false
	sm.currConfig = 0

	sm.configsMu = &sync.Mutex{}
	
	go sm.HandleApplyMsg()
	go sm.HandleLostLeader()
	
	return sm
}
