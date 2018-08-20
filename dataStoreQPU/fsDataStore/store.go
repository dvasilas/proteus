package store

import (
	"log"
	"os"

	utils "github.com/dimitriosvasilas/modqp"
	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
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

//GetPath ...
func (ds FSDataStore) GetPath() string {
	return ds.path
}

//GetSnapshot ...
func (ds FSDataStore) GetSnapshot(msg chan *pbQPU.Object, done chan bool) error {
	f, err := os.Open(ds.path)
	if err != nil {
		return err
	}
	files, err := f.Readdir(-1)
	if err != nil {
		return err
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
	return nil
}

func (ds FSDataStore) watchFS(w *fsnotify.Watcher, msg chan *pbQPU.Operation, done chan bool, stopped chan bool) {
	for {
		select {
		case event := <-w.Events:
			done <- false
			f, err := os.Stat(event.Name)
			if err != nil {
				log.Fatalf("%v", err)
			}
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
			stopped <- true
			log.Fatalf("fsnotify error: %v", err)
		}
	}
}

//SubscribeOps ...
func (ds FSDataStore) SubscribeOps(msg chan *pbQPU.Operation, done chan bool) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	stopped := make(chan bool)

	go ds.watchFS(watcher, msg, done, stopped)

	err = watcher.Add(ds.path)
	if err != nil {
		log.Fatal(err)
	}
	<-stopped
	done <- true
	return nil
}
