package libqpu

import (
	"errors"
	"strconv"

	"github.com/dvasilas/proteus/internal/libqpu/utils"
	"github.com/dvasilas/proteus/internal/proto/qpu"
)

// Schema ...
type Schema map[string]SchemaTable

// SchemaTable ...
type SchemaTable struct {
	Attributes      map[string]DatastoreAttributeType
	DownstreamQuery DownstreamQueryConf
}

// DownstreamQueryConf ...
type DownstreamQueryConf struct {
	IsNull    []string
	IsNotNull []string
}

// DatastoreAttributeType ...
type DatastoreAttributeType int

const (
	// STR ...
	STR DatastoreAttributeType = iota
	// INT ...
	INT DatastoreAttributeType = iota
	// FLT ...
	FLT DatastoreAttributeType = iota
)

// StrToAttributes ...
func (s Schema) StrToAttributes(table string, attributesStr map[string]string) (map[string]*qpu.Value, error) {
	result := make(map[string]*qpu.Value)
	for key, val := range attributesStr {
		value, err := s.StrToValue(table, key, val)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

// InterfaceToAttributes ...
func (s Schema) InterfaceToAttributes(table string, attributesStr map[string]interface{}) (map[string]*qpu.Value, error) {
	result := make(map[string]*qpu.Value)
	for key, val := range attributesStr {
		value, err := s.InterfaceToValue(table, key, val)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

// InterfaceToString ...
func (s Schema) InterfaceToString(table string, attributesStr map[string]interface{}) (map[string][]byte, error) {
	result := make(map[string][]byte)
	for key, val := range attributesStr {
		value, err := s.InterfaceToStr(table, key, val)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

// HasAttribute ...
func HasAttribute(attributes map[string]*qpu.Value, attrName string) bool {
	_, found := attributes[attrName]
	return found
}

// GetValue ...
func (s Schema) GetValue(attributes map[string]*qpu.Value, table, attrName string) (interface{}, error) {
	val, found := attributes[attrName]
	if !found {
		return nil, utils.Error(errors.New("attribute not in attributes map"))
	}
	tbl, found := s[table]
	if !found {
		return nil, utils.Error(errors.New("unknown table: not in schema"))
	}
	attrType, found := tbl.Attributes[attrName]
	if !found {
		return nil, utils.Error(errors.New("unknown attribute: not in schema"))
	}
	switch val.GetVal().(type) {
	case *qpu.Value_Str:
		if attrType != STR {
			return nil, utils.Error(errors.New("attribute value type mismatch"))
		}
		return val.GetStr(), nil
	case *qpu.Value_Int:
		if attrType != INT {
			return nil, utils.Error(errors.New("attribute value type mismatch"))
		}
		return val.GetInt(), nil
	case *qpu.Value_Flt:
		if attrType != FLT {
			return nil, utils.Error(errors.New("attribute value type mismatch"))
		}
		return val.GetFlt(), nil
	default:
		return nil, utils.Error(errors.New("unknown value type"))
	}
}

// StrToValue ...
func (s Schema) StrToValue(table, attributeKey, valueStr string) (*qpu.Value, error) {
	switch s[table].Attributes[attributeKey] {
	case STR:
		return ValueStr(valueStr), nil
	case INT:
		val, err := strconv.ParseInt(valueStr, 10, 32)
		if err != nil {
			return nil, err
		}
		return ValueInt(int32(val)), nil
	case FLT:
		val, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return nil, err
		}
		return ValueFlt(val), nil
	default:
		return ValueStr(valueStr), utils.Error(errors.New("schema: attribute type conversion not implemented"))
	}
}

// InterfaceToStr ...
func (s Schema) InterfaceToStr(table, attributeKey string, val interface{}) ([]byte, error) {
	switch s[table].Attributes[attributeKey] {
	case STR:
		return val.([]byte), nil
	case INT:
		return []byte(strconv.FormatInt(val.(int64), 10)), nil
	case FLT:
		return nil, utils.Error(errors.New("FLT to string conversion not implemented"))
	default:
		return nil, utils.Error(errors.New("schema: attribute type conversion not implemented"))
	}
}

// InterfaceToValue ...
func (s Schema) InterfaceToValue(table, attributeKey string, val interface{}) (*qpu.Value, error) {
	switch s[table].Attributes[attributeKey] {
	case STR:
		return ValueStr(string(val.([]byte))), nil
	case INT:
		return ValueInt(val.(int32)), nil
	case FLT:
		return ValueFlt(val.(float64)), nil
	default:
		return ValueStr(""), utils.Error(errors.New("schema: attribute type conversion not implemented"))
	}
}
