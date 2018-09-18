package utils

import (
	"strconv"

	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
)

//DownwardConns ...
type DownwardConns struct {
	DBs map[int]*DB
}

//DB ...
func (c *DownwardConns) DB(ID int) (db *DB) {
	if c.DBs == nil {
		c.DBs = map[int]*DB{}
	}
	if db = c.DBs[ID]; db == nil {
		db = &DB{}
		c.DBs[ID] = db
	}
	return
}

//DB ...
type DB struct {
	DCs map[int]*DC
}

//DC ...
func (db *DB) DC(ID int) (r *DC) {
	if db.DCs == nil {
		db.DCs = map[int]*DC{}
	}
	if r = db.DCs[ID]; r == nil {
		r = &DC{}
		db.DCs[ID] = r
	}
	return
}

//DC ...
type DC struct {
	Shards map[int]*Shard
}

//Shard ...
func (r *DC) Shard(ID int) (s *Shard) {
	if r.Shards == nil {
		r.Shards = map[int]*Shard{}
	}
	if s = r.Shards[ID]; s == nil {
		s = &Shard{}
		r.Shards[ID] = s
	}
	return
}

//Shard ...
type Shard struct {
	QPUs []QPUConn
}

//QPU ...
func (sh *Shard) QPU(c cli.Client, qType string, attr string, lb *pbQPU.Value, ub *pbQPU.Value) {
	q := QPUConn{
		Client:    c,
		QpuType:   qType,
		Attribute: attr,
		Lbound:    lb,
		Ubound:    ub,
	}
	if sh.QPUs == nil {
		sh.QPUs = []QPUConn{q}
	} else {
		sh.QPUs = append(sh.QPUs, q)
	}
	return
}

//QPUConn ...
type QPUConn struct {
	Client    cli.Client
	QpuType   string
	Attribute string
	Lbound    *pbQPU.Value
	Ubound    *pbQPU.Value
}

//QPUConfig ...
type QPUConfig struct {
	QpuType string
	Port    string
	Conns   []struct {
		DataSet struct {
			DB    int
			DC    int
			Shard int
		}
		EndPoint string
		QpuType  string
		Config   struct {
			DataType  string
			Attribute string
			Ubound    string
			Lbound    string
		}
	}
	Config struct {
		DataType  string
		Attribute string
		LBound    string
		UBound    string
	}
}

//NewDConn ...
func NewDConn(conf QPUConfig) (DownwardConns, error) {
	var dConns DownwardConns
	for _, conn := range conf.Conns {
		c, _, err := cli.NewClient(conn.EndPoint)
		if err != nil {
			return DownwardConns{}, err
		}
		var lb *pbQPU.Value
		var ub *pbQPU.Value
		switch conn.Config.DataType {
		case "string":
			lb = ValStr(conn.Config.Lbound)
			ub = ValStr(conn.Config.Ubound)
		case "int":
			lbI, _ := strconv.ParseInt(conn.Config.Lbound, 10, 64)
			lb = ValInt(lbI)
			ubI, _ := strconv.ParseInt(conn.Config.Ubound, 10, 64)
			ub = ValInt(ubI)
		}
		dConns.DB(conn.DataSet.DB).DC(conn.DataSet.DC).Shard(conn.DataSet.Shard).QPU(c, conn.QpuType, conn.Config.Attribute, lb, ub)
		//clients = append(clients, dConn)
	}
	return dConns, nil
}

//NewQPUConn ...
func NewQPUConn(c cli.Client) QPUConn {
	return QPUConn{Client: c}
}

//ValInt ...
func ValInt(i int64) *pbQPU.Value {
	return &pbQPU.Value{Val: &pbQPU.Value_Int{Int: i}}
}

//ValStr ...
func ValStr(s string) *pbQPU.Value {
	return &pbQPU.Value{Val: &pbQPU.Value_Name{Name: s}}
}
