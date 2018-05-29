//go:generate protoc -I ./datastore ./datastore/store.proto --go_out=plugins=grpc:./datastore
//go:generate mockgen -destination=mocks/ds_mock.go github.com/dimitriosvasilas/modqp/dataStore/datastore DataStoreClient

package gen
