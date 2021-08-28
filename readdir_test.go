package hdfs

import (
	"fmt"
	"github.com/beyondstorage/go-endpoint"
	"math/rand"
	"path"
	"testing"

	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	tmpDir := t.TempDir()

	host:="127.0.0.1"
	port:=9000

	s, err := newStorager(
		ps.WithEndpoint(endpoint.NewTCP(host,port).String()),
		ps.WithWorkDir(tmpDir),
	)

	require.NoError(t, err)

	err = s.hdfs.MkdirAll(tmpDir, 0666)

	require.NoError(t, err)

	for i := 0; i < numbers; i++ {

		filename := uuid.New().String()

		f, err := s.hdfs.Create(path.Join(s.workDir, filename))

		require.NoError(t, err)

		defer func() {
			closeErr := f.Close()
			if err == nil {
				err = closeErr
			}
		}()

		require.NoError(t, err)
	}

	expected := make(map[string]string)
	fi, err := s.hdfs.ReadDir(tmpDir)
	require.NoError(t, err)

	for _, v := range fi {
		expected[v.Name()] = tmpDir
	}

	actual := make(map[string]string)
	it, err := s.List(s.workDir)
	require.NoError(t, err)

	for {
		o, err := it.Next()
		if err == types.IterateDone {
			break
		}
		_, exist := actual[o.Path]
		if exist {
			require.True(t, exist)
			return
		}

		actual[o.Path] = o.ID
	}
	assert.Equal(t, expected, actual)

}
