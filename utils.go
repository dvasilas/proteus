package utils

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dvasilas/proteus/attributes"
	"github.com/dvasilas/proteus/config"
	pbQPU "github.com/dvasilas/proteus/protos/qpu"
	pb "github.com/dvasilas/proteus/protos/utils"
	cli "github.com/dvasilas/proteus/qpu/client"
)

//QPUConn ...
type QPUConn struct {
	Client    cli.Client
	QpuType   string
	DataType  string
	Attribute string
	Lbound    *pb.Value
	Ubound    *pb.Value
}

//Posting ...
type Posting struct {
	Object  pb.Object
	Dataset pb.DataSet
}

//DownwardConns ...
type DownwardConns struct {
	DBs map[string]*DB
}

//DB ...
type DB struct {
	DCs map[string]*DC
}

//DC ...
type DC struct {
	Shards map[string]*Shard
}

//Shard ...
type Shard struct {
	QPUs []QPUConn
}

//NewDConn ...
func NewDConn(conf config.QPUConfig) (DownwardConns, error) {
	var dConns DownwardConns
	for _, conn := range conf.Connections {
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

//QPU ...
func (sh *Shard) QPU(c cli.Client, qType string, dt string, attr string, lb *pb.Value, ub *pb.Value) {
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

//ValInt ...
func ValInt(i int64) *pb.Value {
	return &pb.Value{Val: &pb.Value_Int{Int: i}}
}

//ValStr ...
func ValStr(s string) *pb.Value {
	return &pb.Value{Val: &pb.Value_Str{Str: s}}
}

//ValFlt ...
func ValFlt(f float64) *pb.Value {
	return &pb.Value{Val: &pb.Value_Flt{Flt: f}}
}

//AttrToVal ...
func AttrToVal(k string, v string) (string, *pb.Value, error) {
	if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-f-") {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {

		}
		return strings.ToLower(k), ValFlt(f), nil
	} else if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-i-") {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", &pb.Value{}, err
		}
		return strings.ToLower(k), ValInt(i), nil
	} else {
		return strings.ToLower(k), ValStr(v), nil
	}
}

//ValToString converts a Value to string
func ValToString(val *pb.Value) string {
	switch val.Val.(type) {
	case *pb.Value_Int:
		return strconv.Itoa(int(val.GetInt()))
	case *pb.Value_Flt:
		return fmt.Sprintf("%f", val.GetFlt())
	case *pb.Value_Str:
		return val.GetStr()
	default:
		return ""
	}
}

//EncodeIndexEntry encodes an Object and a DataSet
//to an index entry (byte slice) containing in the form object_dataset
func EncodeIndexEntry(obj pb.Object, ds pb.DataSet) ([]byte, error) {
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
	var o pb.Object
	var ds pb.DataSet
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

//QueryInAttrRange checks if given predicate can be satisfied by a QPU based on its querable attribute value bounds
func QueryInAttrRange(conn QPUConn, query []*pb.AttributePredicate) bool {
	for _, p := range query {
		switch conn.Lbound.Val.(type) {
		case *pb.Value_Int:
			if p.Lbound.GetInt() > conn.Ubound.GetInt() || p.Ubound.GetInt() < conn.Lbound.GetInt() {
				return false
			}
		case *pb.Value_Str:
			if conn.Ubound.GetStr() != "any" && (p.Lbound.GetStr() > conn.Ubound.GetStr() || p.Ubound.GetStr() < conn.Lbound.GetStr()) {
				return false
			}
		case *pb.Value_Flt:
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
func CanProcessQuery(conn QPUConn, query []*pb.AttributePredicate) bool {
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

//----------- Stream Consumer Functions ------------

//FindResponseConsumer receives a QueryResponseStream, iteratively reads from the stream, and processes each input element based on a given function
func FindResponseConsumer(pred []*pb.AttributePredicate, streamIn pbQPU.QPU_FindClient, streamOut pbQPU.QPU_FindServer, errs chan error, process func([]*pb.AttributePredicate, *pbQPU.FindResponseStream, pbQPU.QPU_FindServer) error) {
	for {
		streamMsg, err := streamIn.Recv()
		if err == io.EOF {
			errs <- nil
			return
		} else if err != nil {
			errs <- err
			return
		}
		if err = process(pred, streamMsg, streamOut); err != nil {
			errs <- err
			return
		}
	}
}
