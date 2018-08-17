package index

import (
	"strconv"

	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
)

//Index ...
type Index struct {
	attribute string
	lbound    int64
	ubound    int64
	entries   map[string][]pbQPU.Object
}

//New ...
func New(attr string, lb int64, ub int64) *Index {
	return &Index{
		attribute: attr,
		lbound:    lb,
		ubound:    ub,
		entries:   make(map[string][]pbQPU.Object),
	}
}

//FilterIndexable ...
func (i *Index) FilterIndexable(op *pbQPU.Operation) bool {
	if attrValue, ok := op.Object.Attributes[i.attribute]; ok {
		if attrValue > i.lbound && attrValue <= i.ubound {
			return true
		}
	}
	return false
}

func (i *Index) indexTermKey(op *pbQPU.Operation) string {
	key := i.attribute + "/"
	key += strconv.FormatInt(op.Object.Attributes[i.attribute], 10)
	return key
}

//PredicateToKey ...
func (i *Index) predicateToKey(p *pbQPU.Predicate) string {
	if p.Lbound == p.Ubound {
		return p.Attribute + "/" + strconv.FormatInt(p.Lbound, 10)
	}
	return ""
}

//Put ...
func (i *Index) Put(op *pbQPU.Operation) error {
	key := i.indexTermKey(op)
	if indEntry, ok := i.entries[key]; ok {
		indEntry = append(indEntry, *op.Object)
	} else {
		i.entries[key] = append(indEntry, *op.Object)
	}
	return nil
}

//Get ...
func (i *Index) Get(p []*pbQPU.Predicate) ([]pbQPU.Object, bool, error) {
	key := i.predicateToKey(p[0])
	if indEntry, ok := i.entries[key]; ok {
		return indEntry, true, nil
	}
	return nil, false, nil
}
