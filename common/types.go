package common

type BlockID string

type FileInfo struct {
	Path   string // DFS 内路径
	Blocks []BlockID
}

type BlockLocation struct {
	ID      BlockID
	Address string // storage 节点地址
}

// Master RPC
type CreateFileArgs struct {
	Path   string
	Blocks []BlockID
}
type CreateFileReply struct{ OK bool }
type GetBlocksArgs struct{ Path string }
type GetBlocksReply struct{ Locations []BlockLocation }

// Storage RPC
type StoreBlockArgs struct {
	ID   BlockID
	Data []byte
}
type StoreBlockReply struct{ OK bool }
type FetchBlockArgs struct{ ID BlockID }
type FetchBlockReply struct{ Data []byte }
