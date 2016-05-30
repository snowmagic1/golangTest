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
	
	return ck
}

func (ck *Clerk) NewID() int {
	
	ck.currIdMu.Lock()
	ck.currId ++
	newId := ck.id*1000 + ck.currId
	ck.currIdMu.Unlock()
	
	return newId
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ID = ck.NewID()
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				fmt.Printf("------ client, query Index [%v]=[%v] -----\n", args.ID, reply.Config)
				return reply.Config
			}
			
			// fmt.Printf("------ client, query server %v, ok %v wrong leader %v -----\n", 
			//	i, ok, reply.WrongLeader)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ID = ck.NewID()
	
	fmt.Printf("------ client, join Index [%v] servers [%v] -----\n", args.ID, args.Servers)
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ID = ck.NewID()
	fmt.Printf("------ client, leave Index [%v] servers [%v] -----\n", args.ID, args.GIDs)
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ID = ck.NewID()
	
	fmt.Printf("------ client, move Index [%v] [%v]->[%v]-----\n", args.ID, args.Shard, args.GID)
	
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
