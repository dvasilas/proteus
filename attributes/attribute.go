package attribute

import (
	"errors"
	"strconv"
	"strings"

	utils "github.com/dimitriosvasilas/proteus"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
)

//Attribute interface representing a secondary attribte
//used for implementing polymorphic fucntions
type Attribute interface {
	GetValue(key string, obj *pbQPU.Object) interface{}
	GetDatatype() string
	GetKey(key string) string
	BoundStrToVal(lBound string, uBound string) (*pbQPU.Value, *pbQPU.Value, error)
}

//Attr creates a new instance of the Attribute interface
//given an attribute key argument
func Attr(key string, obj *pbQPU.Object) (Attribute, error) {
	switch key {
	case "size":
		return new(Size), nil
	case "key":
		return new(Key), nil
	default:
		if strings.HasPrefix(key, "tagF") {
			return new(TagF), nil
		} else if _, ok := obj.GetAttributes()["x-amz-meta-f-"+key]; ok {
			return new(TagF), nil
		} else {
			return nil, errors.New("unknown attribute type")
		}
	}
}

//Size implements a size:int attribute
type Size struct{}

//GetValue returns the int value of a size attribute
func (attr *Size) GetValue(key string, obj *pbQPU.Object) interface{} {
	return obj.GetAttributes()[key].GetInt()
}

//GetDatatype returns the attributes datatype
func (attr *Size) GetDatatype() string {
	return "int"
}

//GetKey returns the attributes key (name)
func (attr *Size) GetKey(key string) string {
	return key
}

//BoundStrToVal converts the bounds of a predicate given in string form to pbQPU.Value form
func (attr *Size) BoundStrToVal(lBound string, uBound string) (*pbQPU.Value, *pbQPU.Value, error) {
	lbI, err := strconv.ParseInt(lBound, 10, 64)
	if err != nil {
		return nil, nil, err
	}
	lb := utils.ValInt(lbI)
	ubI, err := strconv.ParseInt(uBound, 10, 64)
	if err != nil {
		return nil, nil, err
	}
	ub := utils.ValInt(ubI)
	return lb, ub, nil
}

//Key implements a key:string attribute
type Key struct{}

//GetValue returns the string value of a key attribute
func (attr *Key) GetValue(key string, obj *pbQPU.Object) interface{} {
	return obj.GetKey()
}

//GetDatatype returns the attributes datatype
func (attr *Key) GetDatatype() string {
	return "key"
}

//GetKey returns the attributes key (name)
func (attr *Key) GetKey(key string) string {
	return key
}

//BoundStrToVal converts the bounds of a predicate given in string form to pbQPU.Value form
func (attr *Key) BoundStrToVal(lBound string, uBound string) (*pbQPU.Value, *pbQPU.Value, error) {
	lb := utils.ValStr(lBound)
	ub := utils.ValStr(uBound)
	return lb, ub, nil
}

//TagF implements a tagF_<key>:float attribute
type TagF struct{}

//GetValue returns the float value of a tagF attribute
func (attr *TagF) GetValue(key string, obj *pbQPU.Object) interface{} {
	return obj.GetAttributes()["x-amz-meta-f-"+key].GetFlt()
}

//GetDatatype returns the attributes datatype
func (attr *TagF) GetDatatype() string {
	return "float"
}

//GetKey returns the attributes key (name)
func (attr *TagF) GetKey(key string) string {
	return strings.Split(key, "_")[1]
}

//BoundStrToVal converts the bounds of a predicate given in string form to pbQPU.Value form
func (attr *TagF) BoundStrToVal(lBound string, uBound string) (*pbQPU.Value, *pbQPU.Value, error) {
	lbF, err := strconv.ParseFloat(lBound, 64)
	if err != nil {
		return nil, nil, err
	}
	lb := utils.ValFlt(lbF)
	ubF, err := strconv.ParseFloat(uBound, 64)
	if err != nil {
		return nil, nil, err
	}
	ub := utils.ValFlt(ubF)
	return lb, ub, nil
}
