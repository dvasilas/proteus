package store

import (
	"os"

	pbQPU "github.com/dimitriosvasilas/modqp/qpupb"
	"github.com/spf13/viper"
)

//FSDataStore ...
type FSDataStore struct{}

//GetSnapshot ...
func (ds FSDataStore) GetSnapshot(msg chan *pbQPU.Object, done chan bool) error {
	path := viper.GetString("datastore.fs.dataDir")

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
