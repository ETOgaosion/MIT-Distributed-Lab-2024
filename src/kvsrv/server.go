package kvsrv

import (
	"crypto/md5"
	"encoding/hex"
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type AppendValue struct {
	committed bool
	value string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	clientcount int
	kv map[string]string
	// sacrifice code readability for performance¥
	// appendrequests: clientid -> reqid -> AppendValue
	appendrequests map[int]map[int]AppendValue
}

func Compress(text string, way bool) string {
	if way {
		hash := md5.Sum([]byte(text))
		return hex.EncodeToString(hash[:])
	}
	if len(text) > 10 {
		return text[:10]
	}
	return text
}


func (kv *KVServer) GetClientID(args *ClientIDArgs, reply *ClientIDReply) {
	// Your code here.
	kv.mu.Lock()
	// if we use retry, we must need extra data structure to store the given client id and the client must do confirmation, cost too much
	kv.clientcount++
	reply.ClientId = kv.clientcount
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("Get request: key: %s, client ID: %d, retry: %t", args.Key, args.ClientId, args.Retry)
	kv.mu.Lock()
	reply.Value = kv.kv[args.Key]
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// DPrintf("Put request: key: %s, value: %s, client id: %d, retry: %t", args.Key, args.Value, args.ClientId, args.Retry)
	kv.mu.Lock()
	kv.kv[args.Key] = args.Value
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// DPrintf("Append request: key: %s, value: %s, client id: %d, retry: %t", args.Key, args.Value, args.ClientId, args.Retry)
	kv.mu.Lock()
	for k := range kv.appendrequests[args.ClientId] {
		if k != args.PutAppendReqId {
			delete(kv.appendrequests[args.ClientId], k)
		}
	}
	if _, ok := kv.appendrequests[args.ClientId]; !ok {
		kv.appendrequests[args.ClientId] = make(map[int]AppendValue)
		kv.appendrequests[args.ClientId][args.PutAppendReqId] = AppendValue{true, kv.kv[args.Key]}
		reply.Value = kv.kv[args.Key]
		kv.kv[args.Key] += args.Value
	} else {
		if value, ok := kv.appendrequests[args.ClientId][args.PutAppendReqId]; !ok {
			// new client, key or value
			kv.appendrequests[args.ClientId][args.PutAppendReqId] = AppendValue{true, kv.kv[args.Key]}
			reply.Value = kv.kv[args.Key]
			kv.kv[args.Key] += args.Value
		} else {
			// client, key and value exist
			if args.Retry && value.committed {
				// !======= Real Duplication Handling =========!
				// this means that the value has been put into kv storage
				// we do nothing with kv, but we should return the old value stored in past appendrequest
				reply.Value = value.value
			} else {
				// !======= Case 1 ==========!
				// last request fail in sending state
				// we still need to execute
				// !======= Case 2 ==========!
				// this request is new, it happens to be same with arguments, update the storage request
				kv.appendrequests[args.ClientId][args.PutAppendReqId] = AppendValue{true, kv.kv[args.Key]}
				reply.Value = kv.kv[args.Key]
				kv.kv[args.Key] += args.Value
			}
		}
	}
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.clientcount = 0
	kv.kv = make(map[string]string)
	kv.appendrequests = make(map[int]map[int]AppendValue)

	return kv
}
