package main

import (
	"errors"
	"net"
	"net/rpc"
	"sync"

	"Min-DFS/common"
)

type Master struct {
	mu       sync.Mutex
	files    map[string][]common.BlockLocation
	storages []string
}

func (m *Master) CreateFile(args *common.CreateFileArgs, reply *common.CreateFileReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.files[args.Path]; exists {
		return errors.New("file exists")
	}
	// 简单轮询副本：这里只放 1 份
	var locs []common.BlockLocation
	for _, bid := range args.Blocks {
		addr := m.storages[int(bid[0])%len(m.storages)]
		locs = append(locs, common.BlockLocation{ID: bid, Address: addr})
	}
	m.files[args.Path] = locs
	reply.OK = true
	return nil
}

func (m *Master) GetBlocks(args *common.GetBlocksArgs, reply *common.GetBlocksReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	locs, ok := m.files[args.Path]
	if !ok {
		return errors.New("file not found")
	}
	reply.Locations = locs
	return nil
}

func main() {
	m := &Master{files: make(map[string][]common.BlockLocation), storages: []string{"localhost:9001", "localhost:9002"}}
	rpc.Register(m)
	l, _ := net.Listen("tcp", ":8000")
	rpc.Accept(l)
}
