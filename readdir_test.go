package hdfs

import (
	"fmt"
	"math/rand"
	"path"
	"testing"

	"github.com/beyondstorage/go-endpoint"
	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestHdfsDirerWithFiles(t *testing.T) {
	for i := 0; i < 100; i++ {
		numbers := 1000 + rand.Intn(1000)

		t.Run(fmt.Sprintf("list %d files", numbers), func(t *testing.T) {
			testHdfsReaddir(t, numbers)
		})
	}
}

func testHdfsReaddir(t *testing.T, numbers int) {
	tmpDir := t.TempDir()
	host := "127.0.0.1"
	port := 9000

	s, err := newStorager(
		ps.WithEndpoint(endpoint.NewTCP(host, port).String()),
		ps.WithWorkDir(tmpDir),
	)
	if err != nil {
		t.Errorf("new storager: %v", err)
	}

	err = s.hdfs.MkdirAll(tmpDir, 0666)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < numbers; i++ {

		filename := uuid.New().String()

		f, err := s.hdfs.Create(path.Join(s.workDir, filename))
		if err != nil {
			t.Error(err)
		}

		defer func() {
			closeErr := f.Close()
			if err == nil {
				err = closeErr
			}
		}()

		if err != nil {
			t.Error(err)
		}
	}

	expected := make(map[string]string)
	fi, err := s.hdfs.ReadDir(tmpDir)
	if err != nil {
		t.Error(err)
	}
	for _, v := range fi {
		expected[v.Name()] = tmpDir
	}

	actual := make(map[string]string)
	it, err := s.List(s.workDir)
	if err != nil {
		t.Error(err)
	}

	for {
		o, err := it.Next()
		if err == types.IterateDone {
			break
		}
		_, exist := actual[o.Path]
		if exist {
			t.Errorf("file %s exists already", o.Path)
			return
		}

		actual[o.Path] = o.ID
	}
	assert.Equal(t, expected, actual)

}
