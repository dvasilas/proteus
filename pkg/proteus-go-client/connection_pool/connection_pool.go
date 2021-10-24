package connpool

import (
	"errors"
	"fmt"
	grpcutils "github.com/dvasilas/proteus/internal/grpc"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// https://github.com/couchbase/go-couchbase/blob/master/conn_pool.go

// ConnectionPool Channels as a Pool
// the connections channel practically does all the work here
// getting a connection from the pool is read from the channel
// if it the first time we try to get connection, or all connections are in use
// makeConn creates a new one
// returning a connection is a non-blocking send back into the channel
type ConnectionPool struct {
	host        string
	makeConn    func(host string, tracing bool) (*grpcutils.GrpcClientConn, error)
	connections chan *grpcutils.GrpcClientConn
	// createsem works as a semaphore,
	// the buffer size controls how many total connections we can possibly have outstanding.
	// each time we establish a new connection, we place an item in that channel.
	// when we close a connection, we remove it again.
	createsem chan bool
	bailOut   chan bool
	poolSize  int
	connCount uint64
	tracing   bool
}

var connPoolTimeout = time.Hour * 24 * 30

var errNoPool = errors.New("no connection pool")
var errClosedPool = errors.New("the connection pool is closed")

// ErrTimeout is the error we send when we timeout while waiting for a
// connection to be created
var ErrTimeout = errors.New("timeout waiting to build connection")

// ConnCloserInterval  ...
var connCloserInterval = time.Second * 30

// ConnPoolAvailWaitTime is the amount of time to wait for an existing
// connection from the pool before considering the creation of a new
// one.
var connPoolAvailWaitTime = time.Millisecond

func makeConn(host string, tracing bool) (*grpcutils.GrpcClientConn, error) {
	grpcConn, err := grpcutils.NewClientConn(host, tracing)
	if err != nil {
		fmt.Println(err)
		debug.PrintStack()
		return nil, err
	}
	return grpcConn, err
}

// NewConnectionPool builds a new connection pool
func NewConnectionPool(host string, closer bool, poolSize, poolOverflow int, tracing bool) *ConnectionPool {
	connSize := poolSize
	if closer {
		connSize += poolOverflow
	}
	rv := &ConnectionPool{
		host:        host,
		connections: make(chan *grpcutils.GrpcClientConn, connSize),
		createsem:   make(chan bool, poolSize+poolOverflow),
		makeConn:    makeConn,
		poolSize:    poolSize,
		tracing:     tracing,
	}

	if closer {
		rv.bailOut = make(chan bool, 1)
		go rv.connCloser()
	}
	return rv
}

// Close ...
func (cp *ConnectionPool) Close() (err error) {
	// return an error instead of panicking
	defer func() {
		if recover() != nil {
			err = errors.New("connectionPool.Close error")
		}
	}()
	if cp.bailOut != nil {

		// defensively, we won't wait if the channel is full
		select {
		case cp.bailOut <- false:
		default:
		}
	}
	close(cp.connections)
	for c := range cp.connections {
		c.Close()
	}
	return
}

// Node ...
func (cp *ConnectionPool) Node() string {
	return cp.host
}

// GetWithTimeout ...
func (cp *ConnectionPool) GetWithTimeout(d time.Duration) (rv *grpcutils.GrpcClientConn, err error) {
	if cp == nil {
		return nil, errNoPool
	}

	// short-circuit available connetions.
	select {
	case rv, isopen := <-cp.connections:
		if !isopen {
			return nil, errClosedPool
		}
		atomic.AddUint64(&cp.connCount, 1)
		return rv, nil
	default:
	}

	t := time.NewTimer(connPoolAvailWaitTime)
	defer t.Stop()

	// try to grab an available connection
	// wait patiently for connPoolAvailWaitTime (1ms)
	select {
	case rv, isopen := <-cp.connections:
		if !isopen {
			return nil, errClosedPool
		}
		atomic.AddUint64(&cp.connCount, 1)
		return rv, nil
	case <-t.C:
		// did not get a connection in time

		// Initialize the full timeout (reusing the same timer)
		t.Reset(d)
		select {
		// try again, but also wait for the ability to create a new connection
		case rv, isopen := <-cp.connections:
			if !isopen {
				return nil, errClosedPool
			}
			atomic.AddUint64(&cp.connCount, 1)
			return rv, nil
		case cp.createsem <- true:
			// Build a connection
			rv, err := cp.makeConn(cp.host, cp.tracing)
			// Enable tracing only the first client we create.
			cp.tracing = false
			if err != nil {
				fmt.Println(err)
				debug.PrintStack()
				// On error, release our create hold
				<-cp.createsem
			} else {
				atomic.AddUint64(&cp.connCount, 1)
			}
			return rv, err
		case <-t.C:
			return nil, ErrTimeout
		}
	}
}

// Return ...
func (cp *ConnectionPool) Return(c *grpcutils.GrpcClientConn) {
	if c == nil {
		return
	}

	if cp == nil || cp.connections == nil {
		c.Close()
	}

	defer func() {
		if recover() != nil {
			// if pool has already been
			// closed and we're trying to return a connection,
			//just close the connection.
			c.Close()
		}
	}()

	select {
	case cp.connections <- c:
	default:
		<-cp.createsem
		c.Close()
	}
}

// Get ...
func (cp *ConnectionPool) Get() (*grpcutils.GrpcClientConn, error) {
	return cp.GetWithTimeout(connPoolTimeout)
}

func (cp *ConnectionPool) connCloser() {
	var connCount uint64

	t := time.NewTimer(connCloserInterval)
	defer t.Stop()

	for {
		connCount = cp.connCount

		// the connection pool has been already closed,
		// we write into bailOut in Close()
		// nothing to do here
		select {
		case <-cp.bailOut:
			return
		case <-t.C:
		}
		t.Reset(connCloserInterval)

		// no overflow connections open or sustained requests for connections
		// nothing to do until the next cycle
		if len(cp.connections) <= cp.poolSize ||
			connCloserInterval/connPoolAvailWaitTime < time.Duration(cp.connCount-connCount) {
			continue
		}

		// close overflow connections now that they are not needed
		for c := range cp.connections {
			select {
			case <-cp.bailOut:
				return
			default:
			}

			// bail out if close did not work out
			if !cp.connCleanup(c) {
				return
			}
			if len(cp.connections) <= cp.poolSize {
				break
			}
		}
	}
}

func (cp *ConnectionPool) connCleanup(c *grpcutils.GrpcClientConn) (rv bool) {
	// just in case we are closing a connection after
	// bailOut has been sent but we haven't yet read it
	defer func() {
		if recover() != nil {
			rv = false
		}
	}()
	rv = true

	c.Close()
	<-cp.createsem
	return
}
