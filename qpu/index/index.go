package index

import (
	"fmt"
	"strconv"

	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
)

//Index ...
type Index interface {
	FilterIndexable(op *pbQPU.Operation) bool
	Put(op *pbQPU.Operation) error
	Get(p []*pbQPU.Predicate) ([]pbQPU.Object, bool, error)
}

//IntHashIndex ...
type IntHashIndex struct {
	attribute string
	lbound    int64
	ubound    int64
	entries   map[string][]pbQPU.Object
}

//New ...
func New(attr string, lb int64, ub int64) *IntHashIndex {
	return &IntHashIndex{
		attribute: attr,
		lbound:    lb,
		ubound:    ub,
		entries:   make(map[string][]pbQPU.Object),
	}
}

//FilterIndexable ...
func (i *IntHashIndex) FilterIndexable(op *pbQPU.Operation) bool {
	if attrValue, ok := op.Object.Attributes[i.attribute]; ok {
		if attrValue.GetInt() > i.lbound && attrValue.GetInt() <= i.ubound {
			return true
		}
	}
	return false
}

func (i *IntHashIndex) indexTermKey(op *pbQPU.Operation) string {
	key := i.attribute + "/"
	key += strconv.FormatInt(op.Object.Attributes[i.attribute].GetInt(), 10)
	return key
}

//PredicateToKey ...
func (i *IntHashIndex) predicateToKey(p *pbQPU.Predicate) string {
	if p.Lbound.GetInt() == p.Ubound.GetInt() {
		return p.Attribute + "/" + strconv.FormatInt(p.Lbound.GetInt(), 10)
	}
	return ""
}

func (i *IntHashIndex) print() {
	fmt.Println(i.entries)
}

//Put ...
func (i *IntHashIndex) Put(op *pbQPU.Operation) error {
	key := i.indexTermKey(op)
	if indEntry, ok := i.entries[key]; ok {
		i.entries[key] = append(indEntry, *op.Object)
	} else {
		i.entries[key] = append(indEntry, *op.Object)
	}
	return nil
}

//Get ...
func (i *IntHashIndex) Get(p []*pbQPU.Predicate) ([]pbQPU.Object, bool, error) {
	key := i.predicateToKey(p[0])
	if indEntry, ok := i.entries[key]; ok {
		return indEntry, true, nil
	}
	return nil, false, nil
}
