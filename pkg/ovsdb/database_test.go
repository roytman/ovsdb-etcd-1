package ovsdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
)

func TestMockLock(t *testing.T) {
	expectedResponse := &LockerMock{}
	var expectedError error
	mock := DatabaseMock{
		Response: expectedResponse,
		Error:    expectedError,
	}
	context.Background()
	actualResponse, actualError := mock.GetLock(context.Background(), "id")
	assert.Equal(t, expectedResponse, actualResponse)
	assert.Equal(t, expectedError, actualError)
}

func TestMockAddSchema(t *testing.T) {
	var expectedError error
	mock := DatabaseMock{
		Error: expectedError,
	}
	actualError := mock.AddSchema("", "")
	assert.Equal(t, expectedError, actualError)
}

func TestMockGetData(t *testing.T) {
	var expectedResponse *clientv3.GetResponse
	var expectedError error
	mock := DatabaseMock{
		Error:    expectedError,
		Response: expectedResponse,
	}
	actualResponse, actualError := mock.GetData(common.GenerateDataKey("dbName", "tableName"), true)
	assert.Equal(t, expectedError, actualError)
	assert.Equal(t, expectedResponse, actualResponse)
}

func TestMockGetSchema(t *testing.T) {
	expectedResponse := "val1"
	expectedOk := true
	mock := DatabaseMock{
		Response: expectedResponse,
		Ok:       expectedOk,
	}
	actualResponse, actualOk := mock.GetSchema("")
	assert.Equal(t, expectedOk, actualOk)
	assert.Equal(t, expectedResponse, actualResponse)
}
