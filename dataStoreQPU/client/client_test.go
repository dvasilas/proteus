package client

import (
	"io"
	"testing"
	"time"

	mock "github.com/dimitriosvasilas/modqp/dataStoreQPU/client/mocks"
	pb "github.com/dimitriosvasilas/modqp/dataStoreQPU/protos"
	pbQPU "github.com/dimitriosvasilas/modqp/protos"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var streamMessages = []pb.StateStream{
	{
		Object: &pbQPU.Object{Key: "obj1", Attributes: map[string]int64{"size": 1}},
	},
	{
		Object: &pbQPU.Object{Key: "key", Attributes: map[string]int64{"size": 2}},
	},
}

func TestGetSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	stream := mock.NewMockDataStoreQPU_GetSnapshotClient(ctrl)
	for _, msg := range streamMessages {
		stream.EXPECT().Recv().Return(&msg, nil)
	}
	stream.EXPECT().Recv().Return(nil, io.EOF)

	dsqpuclient := mock.NewMockDataStoreQPUClient(ctrl)
	dsqpuclient.EXPECT().GetSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).Return(stream, nil)

	c := Client{dsClient: dsqpuclient}

	msg := make(chan *pbQPU.Object)
	done := make(chan bool)

	go c.GetSnapshot(time.Now().UnixNano(), msg, done)

	for {
		if doneMsg := <-done; doneMsg {
			return
		}
		res := <-msg
		assert.NotEmpty(t, res, "GetSnapshot return empty result")
		assert.NotNil(t, res.Key, "")
	}
}
