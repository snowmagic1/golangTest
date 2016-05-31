package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "labrpc"
import "crypto/rand"
import "math/big"
import "shardmaster"
import "time"
import "sync"
import "fmt"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	
	id int
	
	currId int
	currIdMu *sync.Mutex
	
	rpcRequest chan interface{}
	rpcMap map[int]chan interface{}
	rpcMu *sync.Mutex
	
	leaderId int
}

var gclientID = 0

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.id = gclientID
	gclientID++
	
	ck.currId = 0
	ck.currIdMu = &sync.Mutex{}
	
	ck.rpcRequest = make(chan interface{})
	ck.rpcMap = make(map[int]chan interface{})
	ck.rpcMu = &sync.Mutex{}
	
	ck.leaderId = -1
	
	go ck.ProcessRpcRequest()
	
	return ck
}

func (ck *Clerk) NewID() int {
	
	ck.currIdMu.Lock()
	ck.currId ++
	newId := ck.id*1000 + ck.currId
	ck.currIdMu.Unlock()
	
	return newId
}

func (ck *Clerk) WrongLeader(reply interface{}) (wrongLeader bool, err Err) {
	
	switch replyType := reply.(type) {
		case GetReply:
			r := reply.(GetReply)
			wrongLeader = r.WrongLeader
			err = r.Err
			
		case PutAppendReply:	
			r := reply.(PutAppendReply)
			wrongLeader = r.WrongLeader
			err = r.Err
						
		default:
			fmt.Printf("client: unknown reply type %v\n", replyType)
	}
	
	return		
}

func (ck *Clerk) RpcCall(server *labrpc.ClientEnd, args interface{}) (ok bool, reply interface{}){
	switch argsType := args.(type) {
			case GetArgs:
				rpcName := "ShardKV.Get"
				r := GetReply{}
				getArgs := args.(GetArgs)
				ok = server.Call(rpcName, &getArgs, &r)
				reply = r
				
			case PutAppendArgs:	
				rpcName := "ShardKV.PutAppend"
				r := PutAppendReply{}
				putappendArgs := args.(PutAppendArgs)
				
				ok = server.Call(rpcName, &putappendArgs, &r)
				reply = r
								
			default:
				fmt.Printf("client: RpcCall, unknown args %v type %v\n", args, argsType)
				return
		}
		
		return
}

func (ck *Clerk) ProcessRpcRequest() {
	
	for {
		
		args:=<-ck.rpcRequest
		var id int
		var key string
		
		switch argsType := args.(type) {
			case GetArgs:
				id = args.(GetArgs).ID
				key = args.(GetArgs).Key
				
			case PutAppendArgs:	
				id = args.(PutAppendArgs).ID
				key = args.(PutAppendArgs).Key
							
			default:
				fmt.Printf("client: unknown args %v type %v\n", args, argsType)
				continue
		}
		
		for {
			shard := key2shard(key)
			gid := ck.config.Shards[shard]
			if servers, ok := ck.config.Groups[gid]; ok {
				// try each server for the shard.
				for si := 0; si < len(servers); si++ {
					srv := ck.make_end(servers[si])
					ok, reply := ck.RpcCall(srv, args)
					wrongLeader, err := ck.WrongLeader(reply)
					
					fmt.Printf("client, RPC call %v args %v, returns wrong leader %v err %v\n",
						si, args, wrongLeader, err)
					
					if ok && wrongLeader == false && (err == OK || err == ErrNoKey) {
						ck.rpcMap[id] <- reply
						return
					}
					if ok && (err == ErrWrongGroup) {
						break
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
			// ask master for the latest configuration.
			ck.config = ck.sm.Query(-1)
		}
	}	
}

func (ck *Clerk) RpcCallAndWait(id int, args interface{}) (reply interface{}) {
	
	ch := make(chan interface{})
	
	ck.rpcMu.Lock()
	ck.rpcMap[id] = ch
	ck.rpcMu.Unlock()
	
	// fmt.Printf("client: enqueue rpc request %v \n", args)
	ck.rpcRequest <- args
	
	r:=<-ch
	reply = r
	
	return
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ID = ck.NewID()
	
	fmt.Printf("\n------> client, Get Index [%v] -----\n", key)
	
	r := ck.RpcCallAndWait(args.ID, args)
	reply := r.(GetReply)
	
	if(reply.Err != OK) {
		fmt.Printf(">> client, Failed to Get [%v] ,%v\n", 
			args.ID, reply.Err)
		
		return ""
	}
	
	fmt.Printf("<------ client, Get Index [%v]=[%v] -----\n\n", key, reply.Value)
	
	return reply.Value
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op OPCode) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ID = ck.NewID()
	
	fmt.Printf("\n------> client, PutAppend Index [%v], [%v] [%v] [%v] -----\n", 
		args.ID, key, op, value)
		
	r := ck.RpcCallAndWait(args.ID, args)
	reply := r.(PutAppendReply)
	
	if(reply.Err != OK) {
		fmt.Printf(">> client, Index [%v] Failed to PutAppend [%v] [%v] [%v],%v\n\n", 
			args.ID, key, op, value, reply.Err)
		
		return 
	}
	
	fmt.Printf("<------ client, PutAppend Index [%v], [%v] [%v] [%v] -----\n", 
		args.ID, key, op, value)
	
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OPPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OPAppend)
}
