package main

import "fmt"
import "sync"
import "io"
import "encoding/binary"

type Client struct {
    mu sync.Mutex
    conn io.ReadWriteCloser
    xid int64
    pending map[int64]chan int32   
}

func MakeClient(conn io.ReadWriteCloser) *Client {
    client := &Client{}
    client.conn = conn
    client.xid = 1
    client.pending = make(map[int64]chan int32)    
    go client.Listen()
    
    return client
} 

func(client *Client) WriteRequest(xid int64, procName, arg int32) {
    binary.Write(client.conn, binary.LittleEndian, xid)
    binary.Write(client.conn, binary.LittleEndian, procName)
    binary.Write(client.conn, binary.LittleEndian, arg)
}

func(client *Client) ReadReply() (int64, int32){
    var xid int64
    var reply int32
    
    binary.Read(client.conn, binary.LittleEndian, &xid)
    binary.Read(client.conn, binary.LittleEndian, &reply)
    
    return xid, reply
}

func(client *Client) Call(procName, arg int32) int32{
    client.mu.Lock()
    xid := client.xid
    client.xid ++
    done := make(chan int32)
    client.pending[xid] = done 
    client.WriteRequest(xid, procName, arg)
    client.mu.Unlock()
    
    reply := <-done
    
    client.mu.Lock()
    delete(client.pending, xid)
    client.mu.Unlock()
    
    return reply
}

func(client *Client) Listen() {
    for {
        xid, reply := client.ReadReply()
        client.mu.Lock()
        done, ok := client.pending[xid]
        client.mu.Unlock()
        
        if(ok) {
            done <- reply
        }
    } 
}

type Server struct {
    mu sync.Mutex
    conn io.ReadWriteCloser
    handler map[int32]func(int32)int32    
}

func(server *Server) ReadRequest() (int64, int32, int32){
    var xid int64
    var procName, arg int32
    
    binary.Read(server.conn, binary.LittleEndian, &xid)
    binary.Read(server.conn, binary.LittleEndian, &procName)
    binary.Read(server.conn, binary.LittleEndian, &arg)
    
    return xid, procName, arg
}

func(server *Server) WriteReply(xid int64, reply int32) {
    binary.Write(server.conn, binary.LittleEndian, xid)
    binary.Write(server.conn, binary.LittleEndian, reply)
}

func(server *Server) Listen() {
    for {
        xid, procName, arg := server.ReadRequest()
        fmt.Printf("xid: %v procName: %v arg: %v \n",xid, procName, arg)
        
        server.mu.Lock()
        fn, ok := server.handler[procName]
        server.mu.Unlock()
        
        go func() {
            var reply int32
            
            if(ok) {
                reply = fn(arg)
            }
            
            server.mu.Lock()
            server.WriteReply(xid, reply)
            server.mu.Unlock()
        }()
    }    
}

func MakeServer(conn io.ReadWriteCloser) *Server{
    server := &Server{}
    server.conn = conn
    server.handler = make(map[int32]func(int32)int32)
    
    go server.Listen()
    
    return server
}

type Pair struct {
    r *io.PipeReader
    w *io.PipeWriter    
}

func (p Pair) Read(data []byte) (int, error) {
  return p.r.Read(data)
}

func (p Pair) Write(data []byte) (int, error) {
  return p.w.Write(data)
}

func (p Pair) Close() error {
  p.r.Close()
  return p.w.Close()
}

func main() {
    fmt.Println("hello")
    
    r1, w1 := io.Pipe()
    r2, w2 := io.Pipe()
    
    clientConn := Pair{r:r1, w:w2}
    serverConn := Pair{r:r2, w:w1}
    
    client := MakeClient(clientConn)
    server := MakeServer(serverConn)
    
    server.handler[11] = func(v int32) int32 {return v+1}
    reply := client.Call(11, 123)
    
    fmt.Println("123 + 1 =  ", reply)
}