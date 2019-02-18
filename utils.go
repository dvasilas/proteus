package utils

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	attribute "github.com/dimitriosvasilas/proteus/attributes"
	config "github.com/dimitriosvasilas/proteus/config"
	dSQPUcli "github.com/dimitriosvasilas/proteus/dataStoreQPU/client"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	cli "github.com/dimitriosvasilas/proteus/qpu/client"
)

//QPUConn ...
type QPUConn struct {
	Client    cli.Client
	QpuType   string
	DataType  string
	Attribute string
	Lbound    *pbQPU.Value
	Ubound    *pbQPU.Value
}

//Posting ...
type Posting struct {
	Object  pbQPU.Object
	Dataset pbQPU.DataSet
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

//NewDConn ...
func NewDConn(conf config.QPUConfig) (DownwardConns, error) {
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

//AttrToVal ...
func AttrToVal(k string, v string) (string, *pbQPU.Value, error) {
	if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-f-") {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {

		}
		return strings.ToLower(k), ValFlt(f), nil
	} else if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-i-") {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", &pbQPU.Value{}, err
		}
		return strings.ToLower(k), ValInt(i), nil
	} else {
		return strings.ToLower(k), ValStr(v), nil
	}
}

//ValToString converts a *pbQPU.Value to string
func ValToString(val *pbQPU.Value) string {
	switch val.Val.(type) {
	case *pbQPU.Value_Int:
		return strconv.Itoa(int(val.GetInt()))
	case *pbQPU.Value_Flt:
		return fmt.Sprintf("%f", val.GetFlt())
	case *pbQPU.Value_Str:
		return val.GetStr()
	default:
		return ""
	}
}

//EncodeIndexEntry encodes an pbQPU.Object and a pbQPU.DataSet
//to an index entry (byte slice) containing in the form object_dataset
func EncodeIndexEntry(obj pbQPU.Object, ds pbQPU.DataSet) ([]byte, error) {
	buff := make([]byte, 0)

	objMarshaled, err := obj.XXX_Marshal(buff, true)
	if err != nil {
		return nil, err
	}

	dsMarshaled, err := ds.XXX_Marshal(buff, true)
	if err != nil {
		return nil, err
	}

	objMarshaled = append(objMarshaled, []byte("_")...)
	objMarshaled = append(objMarshaled, dsMarshaled...)

	return objMarshaled, nil
}

//DecodeIndexEntry decodes an index entry (byte slice) of the form object_dataset
//to a Posting containing corresponding the object and dataset
func DecodeIndexEntry(entry []byte) (Posting, error) {
	var o pbQPU.Object
	var ds pbQPU.DataSet
	objDs := bytes.Split(entry, []byte("_"))

	err := o.XXX_Unmarshal(objDs[0])
	if err != nil {
		return Posting{}, err
	}
	fmt.Println(o)

	err = ds.XXX_Unmarshal(objDs[1])
	if err != nil {
		return Posting{}, err
	}
	return Posting{Object: o, Dataset: ds}, nil
}

//DownwardConns ...
type DownwardConns struct {
	DBs    map[string]*DB
	DsConn []dSQPUcli.Client
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

//QueryInAttrRange checks if given predicate can be satisfied by a QPU based on its querable attribute value bounds
func QueryInAttrRange(conn QPUConn, query []*pbQPU.Predicate) bool {
	for _, p := range query {
		switch conn.Lbound.Val.(type) {
		case *pbQPU.Value_Int:
			if p.Lbound.GetInt() > conn.Ubound.GetInt() || p.Ubound.GetInt() < conn.Lbound.GetInt() {
				return false
			}
		case *pbQPU.Value_Str:
			if conn.Ubound.GetStr() != "any" && (p.Lbound.GetStr() > conn.Ubound.GetStr() || p.Ubound.GetStr() < conn.Lbound.GetStr()) {
				return false
			}
		case *pbQPU.Value_Flt:
			if p.Lbound.GetFlt() > conn.Ubound.GetFlt() || p.Ubound.GetFlt() < conn.Lbound.GetFlt() {
				return false
			}
		default:
			return false
		}
	}
	return true
}

//CanProcessQuery checks if given predicate can be satisfied by a QPU based on its querable attribute
func CanProcessQuery(conn QPUConn, query []*pbQPU.Predicate) bool {
	if conn.Attribute == "any" {
		return true
	}
	for _, p := range query {
		attr, _, err := attribute.Attr(conn.Attribute, nil)
		if err != nil {
			return false
		}
		if p.Datatype != attr.GetDatatype() {
			return false
		}
	}
	return true
}
