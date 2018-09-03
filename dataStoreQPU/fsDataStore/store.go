package store

import (
	"os"

	utils "github.com/dimitriosvasilas/modqp"
	pbQPU "github.com/dimitriosvasilas/modqp/protos/utils"
	"github.com/fsnotify/fsnotify"
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
func (ds FSDataStore) GetSnapshot(msg chan *pbQPU.Object, done chan bool, errs chan error) {
	f, err := os.Open(ds.path)
	if err != nil {
		done <- true
		errs <- err
		return
	}
	files, err := f.Readdir(-1)
	if err != nil {
		done <- true
		errs <- err
		return
	}
	for _, file := range files {
		done <- false
		msg <- &pbQPU.Object{
			Key: file.Name(),
			Attributes: map[string]*pbQPU.Value{
				"size":    utils.ValInt(file.Size()),
				"mode":    utils.ValInt(int64(file.Mode())),
				"modTime": utils.ValInt(file.ModTime().UnixNano()),
			},
		}
	}
	done <- true
	errs <- nil
}

func (ds FSDataStore) watchFS(w *fsnotify.Watcher, msg chan *pbQPU.Operation, done chan bool, errs chan error) {
	for {
		select {
		case event := <-w.Events:
			f, err := os.Stat(event.Name)
			if err != nil {
				done <- true
				errs <- err
				break
			}
			done <- false
			msg <- &pbQPU.Operation{
				Key: event.Name,
				Op:  event.Op.String(),
				Object: &pbQPU.Object{
					Key: f.Name(),
					Attributes: map[string]*pbQPU.Value{
						"size":    utils.ValInt(f.Size()),
						"mode":    utils.ValInt(int64(f.Mode())),
						"modTime": utils.ValInt(f.ModTime().UnixNano()),
					},
				},
			}
		case err := <-w.Errors:
			done <- true
			errs <- err
			break
		}
	}
}

//SubscribeOps ...
func (ds FSDataStore) SubscribeOps(msg chan *pbQPU.Operation, done chan bool, errs chan error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		errs <- err
	}
	defer watcher.Close()

	go ds.watchFS(watcher, msg, done, errs)

	err = watcher.Add(ds.path)
	if err != nil {
		errs <- err
	}
	<-errs
}
