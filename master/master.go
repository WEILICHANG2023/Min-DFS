package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"Min-DFS/common"

	_ "github.com/go-sql-driver/mysql"
)

type Master struct {
	db       *sql.DB
	mu       sync.Mutex
	storages []string
}

func NewMaster(dsn string, storages []string) (*Master, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Master{db: db, storages: storages}, nil
}

func (m *Master) CreateFile(args *common.CreateFileArgs, reply *common.CreateFileReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	// 插入文件记录
	res, err := tx.Exec("INSERT INTO files(path) VALUES(?)", args.Path)
	if err != nil {
		tx.Rollback()
		return errors.New("file exists or DB error: " + err.Error())
	}
	fileID, _ := res.LastInsertId()
	// 批量插入块记录
	stmt, err := tx.Prepare("INSERT INTO blocks(block_id, file_id, address) VALUES(?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, bid := range args.Blocks {
		// 简单轮询选取存储节点
		addr := m.storages[int(bid[0])%len(m.storages)]
		if _, err := stmt.Exec(string(bid), fileID, addr); err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	reply.OK = true
	return nil
}

func (m *Master) GetBlocks(args *common.GetBlocksArgs, reply *common.GetBlocksReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 查找文件ID
	var fileID int64
	err := m.db.QueryRow("SELECT id FROM files WHERE path = ?", args.Path).Scan(&fileID)
	if err == sql.ErrNoRows {
		return errors.New("file not found")
	} else if err != nil {
		return err
	}
	// 查询块位置
	rows, err := m.db.Query("SELECT block_id, address FROM blocks WHERE file_id = ? ORDER BY id", fileID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var locs []common.BlockLocation
	for rows.Next() {
		var bid, addr string
		if err := rows.Scan(&bid, &addr); err != nil {
			return err
		}
		locs = append(locs, common.BlockLocation{ID: common.BlockID(bid), Address: addr})
	}
	reply.Locations = locs
	return nil
}

func main() {
	// 配置 DSN：user:password@tcp(127.0.0.1:3306)/mini_dfs?charset=utf8mb4
	dsn := "root:123456@tcp(127.0.0.1:3306)/mini_dfs?parseTime=true"
	storages := []string{"localhost:9001", "localhost:9002"}
	master, err := NewMaster(dsn, storages)
	if err != nil {
		log.Fatalf("Failed to start master: %v", err)
	}
	rpc.Register(master)
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatalf("Listen error: %v", err)
	}
	fmt.Println("Master listening on :8000")
	rpc.Accept(l)
}
