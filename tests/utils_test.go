package tests

import (
	hdfs "github.com/beyondstorage/go-service-hdfs"
	"github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/google/uuid"
	"os"
	"testing"
)

func setupTest(t *testing.T) types.Storager {
	t.Log("Setup test for HDFS")

	store, err := hdfs.NewStorager(
		pairs.WithEndpoint(os.Getenv("STORAGE_HDFS_ENDPOINT")),
		pairs.WithWorkDir("/"+uuid.New().String()+"/"),
	)
	if err != nil {
		t.Errorf("new storager: %v", err)
	}

	return store
}
