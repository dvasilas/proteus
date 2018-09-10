package utils

import (
	"strconv"

	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
)

//DownwardConn ...
type DownwardConn struct {
	Client    cli.Client
	QpuType   string
	Attribute string
	Lbound    *pbQPU.Value
	Ubound    *pbQPU.Value
}

//QPUConfig ...
type QPUConfig struct {
	QpuType  string
	Hostname string
	Port     string
	Conns    []struct {
		Hostname string
		Port     string
		QpuType  string
		Config   struct {
			Attribute string
			Datatype  string
			Ubound    string
			Lbound    string
		}
	}
	Config struct {
		IndexType string
		Attribute string
		LBound    string
		UBound    string
	}
}

//NewDConn ...
func NewDConn(conf QPUConfig) ([]DownwardConn, error) {
	var clients []DownwardConn
	for _, conn := range conf.Conns {
		c, _, err := cli.NewClient(conn.Hostname + ":" + conn.Port)
		if err != nil {
			return []DownwardConn{}, err
		}
		dConn := DownwardConn{
			Client:    c,
			QpuType:   conn.QpuType,
			Attribute: conn.Config.Attribute,
		}
		switch conn.Config.Datatype {
		case "any":
			dConn.Lbound = ValStr("any")
			dConn.Ubound = ValStr("any")
		case "int":
			lb, _ := strconv.ParseInt(conn.Config.Lbound, 10, 64)
			dConn.Lbound = ValInt(lb)
			ub, _ := strconv.ParseInt(conn.Config.Ubound, 10, 64)
			dConn.Ubound = ValInt(ub)
		}
		clients = append(clients, dConn)
	}
	return clients, nil
}

//NewDConnClient ...
func NewDConnClient(c cli.Client) []DownwardConn {
	return []DownwardConn{{Client: c}}
}

//ValInt ...
func ValInt(i int64) *pbQPU.Value {
	return &pbQPU.Value{Val: &pbQPU.Value_Int{Int: i}}
}

//ValStr ...
func ValStr(s string) *pbQPU.Value {
	return &pbQPU.Value{Val: &pbQPU.Value_Name{Name: s}}
}
