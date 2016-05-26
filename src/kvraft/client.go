package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "fmt"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	
	leaderId int
	
	currId int
	currIdMu *sync.Mutex
	
	rpcRequest chan interface{}
	rpcMap map[int]chan interface{}
	rpcMu *sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = -1
	
	ck.currId = 0
	ck.currIdMu = &sync.Mutex{}
	
	ck.rpcRequest = make(chan interface{})
	ck.rpcMap = make(map[int]chan interface{})
	ck.rpcMu = &sync.Mutex{}
	
	go ck.ProcessRpcRequest()
	
	// You'll have to add code here.
	fmt.Println()
	return ck
}

func (ck *Clerk) NewID() int {
	
	ck.currIdMu.Lock()
	ck.currId ++
	newId := ck.currId
	ck.currIdMu.Unlock()
	
	return newId
}

func (ck *Clerk) WrongLeader(reply interface{}) (wrongLeader bool, serverId int) {
	
	switch replyType := reply.(type) {
		case GetReply:
			r := reply.(GetReply)
			wrongLeader = r.WrongLeader == "true"
			serverId = r.ServerId
			
		case PutAppendReply:	
			r := reply.(PutAppendReply)
			wrongLeader = r.WrongLeader == "true"
			serverId = r.ServerId
			
		default:
			fmt.Printf("client: unknown reply type %v\n", replyType)
	}
	
	return		
}

func (ck *Clerk) RpcCall(server *labrpc.ClientEnd, args interface{}) (ok bool, reply interface{}){
	switch argsType := args.(type) {
			case GetArgs:
				rpcName := "RaftKV.Get"
				r := GetReply{}
				getArgs := args.(GetArgs)
				ok = server.Call(rpcName, &getArgs, &r)
				reply = r
				
			case PutAppendArgs:	
				rpcName := "RaftKV.PutAppend"
				r := PutAppendReply{}
				putApendArgs := args.(PutAppendArgs)
				
				ok = server.Call(rpcName, &putApendArgs, &r)
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
		var reply interface{}
		var id int
		
		switch argsType := args.(type) {
			case GetArgs:
				id = args.(GetArgs).ID
				
			case PutAppendArgs:	
				id = args.(PutAppendArgs).ID
			
			default:
				fmt.Printf("client: unknown args %v type %v\n", args, argsType)
				continue
		}
		
		for {
			// fmt.Printf("client, leaderID %v\n", ck.leaderId)
			if(ck.leaderId == -1) {
				
				for i := 0;i<len(ck.servers);i++ {
					ok, r := ck.RpcCall(ck.servers[i], args)
					reply = r
					
					//fmt.Printf("----> client, %v Get ok %v reply %v\n", 
					//	i, ok, reply)
					
					wrongLeader, serverId := ck.WrongLeader(reply)
					if(!ok || wrongLeader) {
						time.Sleep(500 * time.Millisecond)
						continue
					}
					
					//fmt.Printf("    leader is %v\n", serverId)
					ck.leaderId = serverId
					
					break
				}
			} else {
				
				success := false
				for retry := 0;retry < 5; retry ++ {
					ok, r := ck.RpcCall(ck.servers[ck.leaderId], args)
					reply = r
					
					if(!ok) {
						time.Sleep(500 * time.Millisecond)
						continue
					}
					
					wrongLeader, serverId := ck.WrongLeader(reply)
					
					if(wrongLeader) {
						//fmt.Printf("client, wrong leader %v\n", ck.leaderId)
						ck.leaderId = -1
						
						break;
					}
					
					ck.leaderId = serverId
					success = true
					break
				}
				
				if(!success){
					ck.leaderId = -1
				}
			}
			
			if(ck.leaderId != -1) {
				ck.rpcMap[id] <- reply
				
				break
			}
			
			time.Sleep(1 * time.Second)
		}
	}	
}

func (ck *Clerk) RpcCallAndWait(key string, value string, op OPCode) (reply interface{}) {
	
	id := ck.NewID()
	var args interface{}
	
	switch op {
		case Get:
			a := GetArgs{}
			a.ID = id
			a.Key = key
			
			args = a
			
		case Put,Append:
			a := PutAppendArgs{}
			a.ID = id
			a.Key = key
			a.Value = value
			a.Op = op
			
			args = a
			
		default:
			fmt.Printf("client: unknown op type %v\n", op)
			return
	}
	
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
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	
	r := ck.RpcCallAndWait(key, "", Get)
	reply := r.(GetReply)
	
	if(reply.Err != Err_Success) {
		fmt.Printf(">> client, server %v Failed to Get [%v] ,%v\n", 
			ck.leaderId, key, reply.Err)
		
		return ""
	}
	
	fmt.Printf(">> client, server %v Get [%v] = %v\n", 
		ck.leaderId, key, reply.Value)
				
	// You will have to modify this function.
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op OPCode) {
	
	r := ck.RpcCallAndWait(key, value, op)
	reply := r.(PutAppendReply)
	
	if(reply.Err != Err_Success) {
		fmt.Printf(">> client, server %v Failed to PutAppend [%v]=[%v] ,%v\n", 
			ck.leaderId, key, value, reply.Err)
		
		return
	}
	
	fmt.Printf(">> client, server %v PutAppend [%v] = %v\n", 
		ck.leaderId, key, value)
		
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, Append)
}
