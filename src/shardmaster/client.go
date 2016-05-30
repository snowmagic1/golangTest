package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"
import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id int
	
	currId int
	currIdMu *sync.Mutex
	
	rpcRequest chan interface{}
	rpcMap map[int]chan interface{}
	rpcMu *sync.Mutex
	
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var gclientID = 0

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	
	// Your code here.
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
		case JoinReply:
			r := reply.(JoinReply)
			wrongLeader = r.WrongLeader
			err = r.Err
			
		case LeaveReply:	
			r := reply.(LeaveReply)
			wrongLeader = r.WrongLeader
			err = r.Err
		
		case MoveReply:
			r := reply.(MoveReply)
			wrongLeader = r.WrongLeader
			err = r.Err

		case QueryReply:
			r := reply.(QueryReply)
			wrongLeader = r.WrongLeader
			err = r.Err
						
		default:
			fmt.Printf("client: unknown reply type %v\n", replyType)
	}
	
	return		
}

func (ck *Clerk) RpcCall(server *labrpc.ClientEnd, args interface{}) (ok bool, reply interface{}){
	switch argsType := args.(type) {
			case JoinArgs:
				rpcName := "ShardMaster.Join"
				r := JoinReply{}
				joinArgs := args.(JoinArgs)
				ok = server.Call(rpcName, &joinArgs, &r)
				reply = r
				
			case LeaveArgs:	
				rpcName := "ShardMaster.Leave"
				r := LeaveReply{}
				leaveArgs := args.(LeaveArgs)
				
				ok = server.Call(rpcName, &leaveArgs, &r)
				reply = r

			case MoveArgs:	
				rpcName := "ShardMaster.Move"
				r := MoveReply{}
				moveArgs := args.(MoveArgs)
				
				ok = server.Call(rpcName, &moveArgs, &r)
				reply = r
				
			case QueryArgs:	
				rpcName := "ShardMaster.Query"
				r := QueryReply{}
				queryArgs := args.(QueryArgs)
				
				ok = server.Call(rpcName, &queryArgs, &r)
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
			case JoinArgs:
				id = args.(JoinArgs).ID
				
			case LeaveArgs:	
				id = args.(LeaveArgs).ID

			case MoveArgs:
				id = args.(MoveArgs).ID
				
			case QueryArgs:	
				id = args.(QueryArgs).ID
							
			default:
				fmt.Printf("client: unknown args %v type %v\n", args, argsType)
				continue
		}
		
		for {
			// fmt.Printf("client [%v], leaderID %v\n", ck.id, ck.leaderId)
			if(ck.leaderId == -1) {
				
				for i := 0;i<len(ck.servers);i++ {
					ok, r := ck.RpcCall(ck.servers[i], args)
					reply = r
					
					//fmt.Printf("----> client, %v Get ok %v reply %v\n", 
					//	i, ok, reply)
					
					wrongLeader, _ := ck.WrongLeader(reply)
					if(!ok || wrongLeader) {
						time.Sleep(500 * time.Millisecond)
						continue
					}
					
					fmt.Printf("== client, leader changed to %v \n", i)
					ck.leaderId = i
					
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
					
					wrongLeader, _ := ck.WrongLeader(reply)
					
					if(wrongLeader) {
						fmt.Printf("== client, wrong leader %v\n", ck.leaderId)
						ck.leaderId = -1
						
						break;
					}
					
					// ck.leaderId = serverId
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
			
			time.Sleep(100 * time.Millisecond)
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

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ID = ck.NewID()
	
	r := ck.RpcCallAndWait(args.ID, *args)
	reply := r.(QueryReply)
	
	if(reply.Err != Err_Success) {
		fmt.Printf(">> client, Failed to query [%v] ,%v\n", 
			args.Num, reply.Err)
		
		return Config{}
	}
	
	fmt.Printf("------ client, query Index [%v]=[%v] -----\n", args.ID, reply.Config)
	
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ID = ck.NewID()
	
	r := ck.RpcCallAndWait(args.ID, *args)
	reply := r.(JoinReply)
	
	if(reply.Err != Err_Success) {
		fmt.Printf(">> client, Failed to join [%v] ,%v\n", 
			servers, reply.Err)
		
		return
	}
	
	fmt.Printf("------ client, join Index [%v] servers [%v] -----\n", args.ID, servers)
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ID = ck.NewID()
	
	r := ck.RpcCallAndWait(args.ID, *args)
	reply := r.(LeaveReply)
	
	if(reply.Err != Err_Success) {
		fmt.Printf(">> client, Failed to leave [%v] ,%v\n", 
			gids, reply.Err)
		
		return
	}
	
	fmt.Printf("------ client, leave Index [%v] servers [%v] -----\n", args.ID, args.GIDs)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ID = ck.NewID()	
	
	r := ck.RpcCallAndWait(args.ID, *args)
	reply := r.(MoveReply)
	
	if(reply.Err != Err_Success) {
		fmt.Printf(">> client, Failed to move [%v] ,%v\n", 
			gid, reply.Err)
		
		return
	}
	
	fmt.Printf("------ client, move Index [%v] [%v]->[%v]-----\n", args.ID, args.Shard, args.GID)
}
