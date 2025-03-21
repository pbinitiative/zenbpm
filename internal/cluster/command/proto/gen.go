//go:generate protoc -I. --go-grpc_out=. --go-grpc_opt=paths=source_relative --go_out=. --go_opt=paths=source_relative zencommand.proto
package proto
