is a min distributed filesystem by go

usage:

go run storage/storage.go --port 9001 --dataDir ./storage1/data 

go run storage/storage.go --port 9002 --dataDir ./storage2/data 


go run master/master.go


go run client/client.go --mode put --master localhost:8000 --src ./local.txt --dst /foo/bar.txt


go run client/client.go --mode ls --master localhost:8000 --src /foo/bar.txt


go run client/client.go --mode get --master localhost:8000 --src /foo/bar.txt --dst ./downloaded.txt
