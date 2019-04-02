package store

import (
	"errors"
	"os"

	"github.com/dvasilas/proteus/protos"
	pbQPU "github.com/dvasilas/proteus/protos/utils"
	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
)

//FSDataStore ...
type FSDataStore struct {
	path string
}

//New ...
func New(path string) FSDataStore {
	return FSDataStore{
		path: path,
	}
}

//GetSnapshot ...
func (ds FSDataStore) GetSnapshot(msg chan *pbQPU.Object) chan error {
	errCh := make(chan error)

	go func() {

		f, err := os.Open(ds.path)
		if err != nil {
			close(msg)
			errCh <- err
			return
		}
		files, err := f.Readdir(-1)
		if err != nil {
			close(msg)
			errCh <- err
			return
		}
		for _, file := range files {
			msg <- protoutils.Object(
				file.Name(),
				map[string]*pbQPU.Value{
					"size":    protoutils.ValueInt(file.Size()),
					"mode":    protoutils.ValueInt(int64(file.Mode())),
					"modTime": protoutils.ValueInt(file.ModTime().UnixNano()),
				},
			)
		}
		close(msg)
		errCh <- nil
	}()
	return errCh
}

func (ds FSDataStore) watchFS(w *fsnotify.Watcher, msg chan *pbQPU.Operation, errs chan error) {
	for {
		select {
		case event := <-w.Events:
			f, err := os.Stat(event.Name)
			if err != nil {
				errs <- err
				break
			}
			msg <- protoutils.OperationState(
				"noId",
				protoutils.Object(
					f.Name(),
					map[string]*pbQPU.Value{
						"size":    protoutils.ValueInt(f.Size()),
						"mode":    protoutils.ValueInt(int64(f.Mode())),
						"modTime": protoutils.ValueInt(f.ModTime().UnixNano()),
					},
				),
				nil,
			)
		case err := <-w.Errors:
			errs <- err
			break
		}
	}
}

//SubscribeOps ...
func (ds FSDataStore) SubscribeOps(msg chan *pbQPU.Operation, ack chan bool, sync bool) (*grpc.ClientConn, chan error) {
	errCh := make(chan error)
	if sync {
		errCh <- errors.New("Not supported")
	} else {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			errCh <- err
			return nil, errCh
		}
		go ds.watchFS(watcher, msg, errCh)
		err = watcher.Add(ds.path)
		if err != nil {
			errCh <- err
			return nil, errCh
		}
	}
	return nil, errCh
}
