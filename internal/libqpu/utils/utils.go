package utils

import (
	"errors"
	"runtime/debug"

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
