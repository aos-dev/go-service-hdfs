package hdfs

import (
	"fmt"
	"math/rand"
	"path"
	"testing"

	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestHdfsDirerWithFiles(t *testing.T) {
	for i := 0; i < 10; i++ {
		numbers := 1000 + rand.Intn(1000)

		t.Run(fmt.Sprintf("list %d files", numbers), func(t *testing.T) {
			testHdfsReaddir(t, numbers)
		})
	}
}

func testHdfsReaddir(t *testing.T, numbers int) {
	s := NewClient(t)

	err := s.hdfs.MkdirAll(s.workDir, 0666)

	assert.NoError(t, err)

	for i := 0; i < numbers; i++ {

		filename := uuid.New().String()

		f, err := s.hdfs.Create(path.Join(s.workDir, filename))

		assert.NoError(t, err)
		assert.NotNil(t, f)

		err = f.Close()
		assert.NoError(t, err)
	}

	expected := make(map[string]string)
	fi, err := s.hdfs.ReadDir(s.workDir)
	assert.NoError(t, err)
	assert.NotNil(t, fi)

	for _, v := range fi {
		expected[v.Name()] = s.workDir
	}

	actual := make(map[string]string)
	it, err := s.List(s.workDir)
	assert.NoError(t, err)
	assert.NotNil(t, it)

	for {
		o, err := it.Next()

		if err == types.IterateDone {
			break
		}

		assert.NotNil(t, o)

		_, exist := actual[o.Path]
		if exist {
			assert.True(t, exist)
			return
		}

		actual[o.Path] = o.ID
	}
	assert.Equal(t, expected, actual)

}
