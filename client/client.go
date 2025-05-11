package main

import (
	"Min-DFS/common"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"sync"
)

const chunkSize = 64 * 1024

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func put(masterAddr, localPath, dfsPath string) {
	data, err := os.ReadFile(localPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Read local file error:", err)
		return
	}
	var blocks []common.BlockID
	for i := 0; i < len(data); i += chunkSize {
		id := common.BlockID(fmt.Sprintf("blk_%s_%d", dfsPath, i))
		blocks = append(blocks, id)
	}
	mc, err := rpc.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Dial master error:", err)
		return
	}
	defer mc.Close()
	var creply common.CreateFileReply
	if err := mc.Call("Master.CreateFile", &common.CreateFileArgs{Path: dfsPath, Blocks: blocks}, &creply); err != nil || !creply.OK {
		fmt.Fprintln(os.Stderr, "Master.CreateFile error:", err)
		return
	}
	var wg sync.WaitGroup
	for idx, bid := range blocks {
		wg.Add(1)
		go func(i int, blkID common.BlockID) {
			defer wg.Done()
			blockData := data[i*chunkSize : min((i+1)*chunkSize, len(data))]
			var greply common.GetBlocksReply
			if err := mc.Call("Master.GetBlocks", &common.GetBlocksArgs{Path: dfsPath}, &greply); err != nil {
				fmt.Fprintln(os.Stderr, "GetBlocks error:", err)
				return
			}
			for _, loc := range greply.Locations {
				if loc.ID == blkID {
					sc, err := rpc.Dial("tcp", loc.Address)
					if err != nil {
						fmt.Fprintln(os.Stderr, "Dial storage error:", err)
						continue
					}
					var sreply common.StoreBlockReply
					if err := sc.Call("Storage.StoreBlock", &common.StoreBlockArgs{ID: blkID, Data: blockData}, &sreply); err != nil || !sreply.OK {
						fmt.Fprintln(os.Stderr, "StoreBlock error:", err)
					}
					sc.Close()
				}
			}
		}(idx, bid)
	}
	wg.Wait()
	fmt.Println("Upload complete")
}

func get(masterAddr, dfsPath, localPath string) {
	mc, err := rpc.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Dial master error:", err)
		return
	}
	defer mc.Close()
	var greply common.GetBlocksReply
	if err := mc.Call("Master.GetBlocks", &common.GetBlocksArgs{Path: dfsPath}, &greply); err != nil {
		fmt.Fprintln(os.Stderr, "GetBlocks error:", err)
		return
	}
	var data []byte
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, loc := range greply.Locations {
		wg.Add(1)
		go func(l common.BlockLocation) {
			defer wg.Done()
			sc, err := rpc.Dial("tcp", l.Address)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Dial storage error:", err)
				return
			}
			defer sc.Close()
			var freply common.FetchBlockReply
			if err := sc.Call("Storage.FetchBlock", &common.FetchBlockArgs{ID: l.ID}, &freply); err != nil {
				fmt.Fprintln(os.Stderr, "FetchBlock error:", err)
				return
			}
			mu.Lock()
			data = append(data, freply.Data...)
			mu.Unlock()
		}(loc)
	}
	wg.Wait()
	if err := os.WriteFile(localPath, data, 0644); err != nil {
		fmt.Fprintln(os.Stderr, "Write local file error:", err)
		return
	}
	fmt.Println("Download complete")
}

func ls(masterAddr, dfsPath string) {
	mc, err := rpc.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Dial master error:", err)
		return
	}
	defer mc.Close()
	var greply common.GetBlocksReply
	if err := mc.Call("Master.GetBlocks", &common.GetBlocksArgs{Path: dfsPath}, &greply); err != nil {
		fmt.Fprintln(os.Stderr, "GetBlocks error:", err)
		return
	}
	for _, loc := range greply.Locations {
		fmt.Println(loc.ID, "->", loc.Address)
	}
}

func main() {
	mode := flag.String("mode", "put", `"put" or "get" or "ls"`)
	master := flag.String("master", "localhost:8000", "Master address")
	src := flag.String("src", "", "Local or DFS path")
	dst := flag.String("dst", "", "DFS or Local path")
	flag.Parse()
	switch *mode {
	case "put":
		put(*master, *src, *dst)
	case "get":
		get(*master, *src, *dst)
	case "ls":
		ls(*master, *src)
	default:
		fmt.Println("unknown mode")
	}
}
