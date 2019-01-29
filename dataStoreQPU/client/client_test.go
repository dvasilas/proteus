package client

import (
	"io"
	"testing"
	"time"

	utils "github.com/dimitriosvasilas/proteus"
	mock "github.com/dimitriosvasilas/proteus/dataStoreQPU/client/mocks"
	pb "github.com/dimitriosvasilas/proteus/protos/datastore"
	pbQPU "github.com/dimitriosvasilas/proteus/protos/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var streamMessages = []pb.StateStream{
	{
		Object: &pbQPU.Object{Key: "obj1", Attributes: map[string]*pbQPU.Value{"size": utils.ValInt(1)}},
	},
	{
		Object: &pbQPU.Object{Key: "key", Attributes: map[string]*pbQPU.Value{"size": utils.ValInt(2)}},
	},
}

func TestGetSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stream := mock.NewMockDataStore_GetSnapshotClient(ctrl)
	for _, msg := range streamMessages {
		stream.EXPECT().Recv().Return(&msg, nil)
	}
	stream.EXPECT().Recv().Return(nil, io.EOF)

	dsqpuclient := mock.NewMockDataStoreClient(ctrl)
	dsqpuclient.EXPECT().GetSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).Return(stream, nil)

	c := Client{dsClient: dsqpuclient}

	streamFrom, cancel, err := c.GetSnapshot(time.Now().UnixNano())
	defer cancel()
	assert.Nil(t, err)
	for {
		streamMsg, err := streamFrom.Recv()
		if err == io.EOF {
			return
		}
		assert.Nil(t, err)
		assert.NotEmpty(t, streamMsg, "GetSnapshot returned empty result")
		assert.NotNil(t, streamMsg.Object.Key, "")
	}
}
