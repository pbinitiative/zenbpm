//go:generate protoc -I../command/proto -I. --go-grpc_out=. --go-grpc_opt=paths=source_relative --go_out=. --go_opt=paths=source_relative zen_cluster.proto
package proto
