package store

import (
	"log"
	"os"

	pbQPU "github.com/dimitriosvasilas/modqp/qpuUtilspb"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

//FSDataStore ...
type FSDataStore struct{}

//GetSnapshot ...
func (ds FSDataStore) GetSnapshot(msg chan *pbQPU.Object, done chan bool) error {
	path := viper.Get("HOME").(string) + viper.GetString("datastore.fs.dataDir")

	f, err := os.Open(path)
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
			Key:        file.Name(),
			Attributes: map[string]int64{"size": file.Size(), "mode": int64(file.Mode()), "modTime": file.ModTime().UnixNano()},
		}
	}
	done <- true
	return nil
}

func watchFS(w *fsnotify.Watcher, msg chan *pbQPU.Operation, done chan bool, stopped chan bool) {
	for {
		select {
		case event := <-w.Events:
			done <- false
			msg <- &pbQPU.Operation{Key: event.Name, Op: event.Op.String()}
		case err := <-w.Errors:
			stopped <- true
			log.Fatalf("fsnotify error: %v", err)
		}
	}
}

//SubscribeOps ...
func (ds FSDataStore) SubscribeOps(msg chan *pbQPU.Operation, done chan bool) error {
	path := viper.Get("HOME").(string) + viper.GetString("datastore.fs.dataDir")

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	stopped := make(chan bool)

	go watchFS(watcher, msg, done, stopped)

	err = watcher.Add(path)
	if err != nil {
		log.Fatal(err)
	}
	<-stopped
	done <- true
	return nil
}
