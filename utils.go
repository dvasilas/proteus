package utils

import (
	"strconv"

	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	cli "github.com/dimitriosvasilas/modqp/qpu/client"
)

//Posting ...
type Posting struct {
	Object  pbQPU.Object
	Dataset pbQPU.DataSet
}

//DownwardConns ...
type DownwardConns struct {
	DBs map[string]*DB
}

//DB ...
func (c *DownwardConns) DB(ID string) (db *DB) {
	if c.DBs == nil {
		c.DBs = map[string]*DB{}
	}
	if db = c.DBs[ID]; db == nil {
		db = &DB{}
		c.DBs[ID] = db
	}
	return
}

//DB ...
type DB struct {
	DCs map[string]*DC
}

//DC ...
func (db *DB) DC(ID string) (r *DC) {
	if db.DCs == nil {
		db.DCs = map[string]*DC{}
	}
	if r = db.DCs[ID]; r == nil {
		r = &DC{}
		db.DCs[ID] = r
	}
	return
}

//DC ...
type DC struct {
	Shards map[string]*Shard
}

//Shard ...
func (r *DC) Shard(ID string) (s *Shard) {
	if r.Shards == nil {
		r.Shards = map[string]*Shard{}
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
func (sh *Shard) QPU(c cli.Client, qType string, dt string, attr string, lb *pbQPU.Value, ub *pbQPU.Value) {
	q := QPUConn{
		Client:    c,
		QpuType:   qType,
		DataType:  dt,
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
	DataType  string
	Attribute string
	Lbound    *pbQPU.Value
	Ubound    *pbQPU.Value
}

//QPUConfig ...
type QPUConfig struct {
	QpuType string
	Port    string
	Conns   []struct {
		EndPoint string
		DataSet  struct {
			DB    string
			DC    string
			Shard string
		}
	}
	IndexConfig struct {
		DataType  string
		Attribute string
		LBound    string
		UBound    string
		ConsLevel string
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
		connConf, err := c.GetConfig()
		if err != nil {
			return DownwardConns{}, err
		}
		dConns.DB(connConf.GetDataset()[0].GetDb()).
			DC(connConf.GetDataset()[0].GetDc()).
			Shard(connConf.GetDataset()[0].GetShard()).
			QPU(c,
				connConf.QPUType,
				connConf.GetSupportedQueries()[0].GetDatatype(),
				connConf.GetSupportedQueries()[0].GetAttribute(),
				connConf.GetSupportedQueries()[0].GetLbound(),
				connConf.GetSupportedQueries()[0].GetUbound())
	}
	return dConns, nil
}

//ValInt ...
func ValInt(i int64) *pbQPU.Value {
	return &pbQPU.Value{Val: &pbQPU.Value_Int{Int: i}}
}

//ValStr ...
func ValStr(s string) *pbQPU.Value {
	return &pbQPU.Value{Val: &pbQPU.Value_Str{Str: s}}
}

//ValFlt ...
func ValFlt(f float64) *pbQPU.Value {
	return &pbQPU.Value{Val: &pbQPU.Value_Flt{Flt: f}}
}

//AttrBoundStrToVal ...
func AttrBoundStrToVal(dataType string, lBound string, uBound string) (*pbQPU.Value, *pbQPU.Value, error) {
	var lb *pbQPU.Value
	var ub *pbQPU.Value
	switch dataType {
	case "int":
		lbI, err := strconv.ParseInt(lBound, 10, 64)
		if err != nil {
			return nil, nil, err
		}
		lb = ValInt(lbI)
		ubI, err := strconv.ParseInt(uBound, 10, 64)
		if err != nil {
			return nil, nil, err
		}
		ub = ValInt(ubI)
	case "float":
		lbF, err := strconv.ParseFloat(lBound, 64)
		if err != nil {
			return nil, nil, err
		}
		lb = ValFlt(lbF)
		ubF, err := strconv.ParseFloat(uBound, 64)
		if err != nil {
			return nil, nil, err
		}
		ub = ValFlt(ubF)
	default:
		lb = ValStr(lBound)
		ub = ValStr(uBound)
	}
	return lb, ub, nil
}
