//go:generate protoc --go_out=plugins=grpc:$GOPATH/src/ ./qpuUtilspb/qpuUtils.proto

package utils

import (
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
)

//ValInt ...
func ValInt(i int64) *pbQPU.Value {
	return &pbQPU.Value{Val: &pbQPU.Value_Int{Int: i}}
}

//ValStr ...
func ValStr(s string) *pbQPU.Value {
	return &pbQPU.Value{Val: &pbQPU.Value_Name{Name: s}}
}
