package client

import (
	"errors"
	"testing"

	pb "github.com/dimitriosvasilas/modqp/dataStore/datastore"
	dsmock "github.com/dimitriosvasilas/modqp/dataStore/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestPutNormalUse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDSClient := dsmock.NewMockDataStoreClient(ctrl)
	c := Client{dsClient: mockDSClient}

	attrs := map[string]string{"test": "attr"}

	mockDSClient.EXPECT().PutObjectMD(
		gomock.Any(),
		&pb.PutObjMDRequest{Key: "obj1", Attributes: attrs},
	).Return(&pb.PutObjMDReply{Message: "OK"}, nil).Times(1)

	resp, _, err := c.PutObjectMD("obj1", attrs)
	if assert.Nil(t, err) {
		assert.Equal(t, resp, "OK", "")
	}
}

func TestPutEmptyAttrs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDSClient := dsmock.NewMockDataStoreClient(ctrl)
	c := Client{dsClient: mockDSClient}

	mockDSClient.EXPECT().PutObjectMD(
		gomock.Any(),
		&pb.PutObjMDRequest{Key: "obj1"},
	).Return(&pb.PutObjMDReply{Message: "OK"}, nil).Times(1)

	resp, _, err := c.PutObjectMD("obj1", make(map[string]string))
	if assert.Nil(t, err) {
		assert.Equal(t, resp, "OK", "")
	}
}

func TestPutEmptyKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDSClient := dsmock.NewMockDataStoreClient(ctrl)
	c := Client{dsClient: mockDSClient}

	_, _, err := c.PutObjectMD("", make(map[string]string))
	if assert.NotNil(t, err) {
		assert.Equal(t, err, errors.New("Key is empty"), "")
	}
}
