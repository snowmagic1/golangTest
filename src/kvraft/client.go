package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	
	leaderId int
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
	
	// You'll have to add code here.
	fmt.Println()
	return ck
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
	
	args := GetArgs{}
	reply := GetReply{}
	
	args.Key = key
	
	for {
		if(ck.leaderId == -1) {
			
			for i, server := range ck.servers {
				ok := server.Call("RaftKV.Get", &args, &reply)
				// fmt.Printf("----> %v Get ok %v reply %v\n", 
				//	i, ok, reply)
					
				if(!ok || reply.WrongLeader == "true") {
					continue
				}
				
				// fmt.Printf("----> leader is %v\n", i)
				ck.leaderId = i
				
				break
			}
		} else {
			
			ok := ck.servers[ck.leaderId].Call("RaftKV.Get", &args, &reply)
			
			if(!ok || reply.WrongLeader == "true") {
				ck.leaderId = -1
			}
		}
		
		if(ck.leaderId != -1) {
			break
		}
		
		time.Sleep(1 * time.Second)
	}
	
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
	
	args := PutAppendArgs{}
	reply := PutAppendReply{}
	
	args.Key = key
	args.Value = value
	args.Op = op
	
	for {
		if(ck.leaderId == -1) {
			
			for i, server := range ck.servers {
				ok := server.Call("RaftKV.PutAppend", &args, &reply)
				//fmt.Printf("----> %v PutAppend ok %v reply %v\n", 
				//	i, ok, reply)
					
				if(!ok || reply.WrongLeader == "true") {
					continue
				}
				
				fmt.Printf("----> leader is %v\n", i)
				ck.leaderId = i
				
				break
			}
		} else {
			
			ok := ck.servers[ck.leaderId].Call("RaftKV.PutAppend", &args, &reply)
			
			if(!ok || reply.WrongLeader == "true") {
				ck.leaderId = -1
			}
		}
		
		if(ck.leaderId != -1) {
			break
		}
		
		time.Sleep(1 * time.Second)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, Append)
}
