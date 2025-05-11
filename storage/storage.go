package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"

	"Min-DFS/common"
)

var (
	port    = flag.String("port", "9001", "storage listen port")
	baseDir = flag.String("dataDir", "./data", "base directory for blocks")
)

type Storage struct{ BaseDir string }

func (s *Storage) StoreBlock(args *common.StoreBlockArgs, reply *common.StoreBlockReply) error {
	// 确保基础目录存在
	if err := os.MkdirAll(s.BaseDir, 0755); err != nil {
		return err
	}
	// 使用 filepath.Join 构造完整文件路径，并创建所有中间目录
	filePath := filepath.Join(s.BaseDir, string(args.ID))
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	// 写入数据块
	if err := os.WriteFile(filePath, args.Data, 0644); err != nil {
		return err
	}
	reply.OK = true
	return nil
}

func (s *Storage) FetchBlock(args *common.FetchBlockArgs, reply *common.FetchBlockReply) error {
	filePath := filepath.Join(s.BaseDir, string(args.ID))
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	reply.Data = data
	return nil
}

func main() {
	flag.Parse()
	s := &Storage{BaseDir: *baseDir}
	rpc.Register(s)
	addr := fmt.Sprintf(":%s", *port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Storage listening on %s, data dir %s", addr, *baseDir)
	rpc.Accept(l)
}
