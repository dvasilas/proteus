package utils

import (
	"errors"
	"math"
	"math/rand"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/dvasilas/proteus/internal/proto/qpu"
	log "github.com/sirupsen/logrus"
)

// Assert ...
func Assert(cond bool, msg string) {
	if !cond {
		panic(errors.New(msg))
	}
}

// Error logs the given error message and the stack trace,
// and returns an error.
func Error(err error) error {
	log.Error(err)
	debug.PrintStack()
	return err
}

// Warn ...
func Warn(err error) {
	log.Error(err)
	debug.PrintStack()
}

// Trace is a wrapper for printing trace messaging
// Requires the qpu service to be launched with trace logging level (-l trace)
func Trace(msg string, fields map[string]interface{}) {
	logFields := log.Fields{}
	for k, v := range fields {
		logFields[k] = v
	}
	log.WithFields(logFields).Trace(msg)
}

// GetValue ...
func GetValue(v *qpu.Value) (interface{}, error) {
	switch v.GetVal().(type) {
	case *qpu.Value_Str:
		return v.GetStr(), nil
	case *qpu.Value_Int:
		return v.GetInt(), nil
	case *qpu.Value_Flt:
		return v.GetFlt(), nil
	default:
		return nil, Error(errors.New("unknown value type"))
	}
}

// ValueToStr ...
func ValueToStr(v *qpu.Value) (string, error) {
	switch v.GetVal().(type) {
	case *qpu.Value_Str:
		return v.GetStr(), nil
	case *qpu.Value_Int:
		return strconv.FormatInt(v.GetInt(), 10), nil
	case *qpu.Value_Flt:
		return "", Error(errors.New("not implemented"))
	default:
		return "", Error(errors.New("unknown value type"))
	}
}

// Compare ...
func Compare(a, b *qpu.Value) (int, error) {
	if valueType(a) != valueType(b) {
		return -1, nil
	}
	const TOLERANCE = 0.000001
	switch a.GetVal().(type) {
	case *qpu.Value_Flt:
		diff := a.GetFlt() - b.GetFlt()
		if diff := math.Abs(diff); diff < TOLERANCE {
			return 0, nil
		}
		if diff < 0 {
			return -1, nil
		}
		return 1, nil
	case *qpu.Value_Int:
		return int(a.GetInt() - b.GetInt()), nil
	case *qpu.Value_Str:
		return strings.Compare(a.GetStr(), b.GetStr()), nil
	}
	return 0, errors.New("unknown Value type")
}

func valueType(v *qpu.Value) int {
	switch v.GetVal().(type) {
	case *qpu.Value_Flt:
		return 0
	case *qpu.Value_Int:
		return 1
	case *qpu.Value_Str:
		return 2
	}
	rand.Seed(time.Now().UTC().UnixNano())
	return rand.Int()
}
