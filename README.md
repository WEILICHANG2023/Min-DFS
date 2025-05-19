is a min distributed filesystem by go

usage:
```
go run storage/storage.go --port 9001 --dataDir ./storage1/data 

go run storage/storage.go --port 9002 --dataDir ./storage2/data 


go run master/master.go


go run client/client.go --mode put --master localhost:8000 --src ./local.txt --dst /foo/bar.txt


go run client/client.go --mode ls --master localhost:8000 --src /foo/bar.txt


go run client/client.go --mode get --master localhost:8000 --src /foo/bar.txt --dst ./downloaded.txt
```
about mysql:

you need:
```
CREATE DATABASE mini_dfs;
USE mini_dfs;
-- 存储文件元信息
CREATE TABLE files (
    id        INT AUTO_INCREMENT PRIMARY KEY,
    path      VARCHAR(255) NOT NULL UNIQUE
);

-- 存储块位置信息
CREATE TABLE blocks (
    id        INT AUTO_INCREMENT PRIMARY KEY,
    block_id  VARCHAR(255) NOT NULL,
    file_id   INT NOT NULL,
    address   VARCHAR(100) NOT NULL,
    FOREIGN KEY (file_id) REFERENCES files(id)
);
```
