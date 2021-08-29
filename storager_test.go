package hdfs

import (
	"bytes"
	"io"
	"path"
	"testing"

	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/stretchr/testify/assert"
)

func TestStorage_String(t *testing.T) {
	s := NewClient(t)
	assert.NotEmpty(t, s.String())
}

func TestStorage_Metadata(t *testing.T) {
	s := NewClient(t)
	m := s.Metadata()
	assert.NotNil(t, m)
	assert.Equal(t, s.workDir, m.WorkDir)
}

func TestStorage_Stat(t *testing.T) {
	s := NewClient(t)

	err := s.hdfs.MkdirAll(s.workDir, 0666)
	assert.NoError(t, err)

	t.Run("statDir", func(t *testing.T) {
		stat, err := s.Stat(s.workDir)
		assert.NoError(t, err)
		assert.True(t, stat.Mode.IsDir())
	})

	fname := "TestFileStat.txt"
	f1, err := s.hdfs.Create(path.Join(s.workDir + fname))
	assert.NoError(t, err)
	assert.NotNil(t, f1)

	stat2, err := s.hdfs.Stat(path.Join(s.workDir + fname))
	assert.NoError(t, err)
	assert.NotNil(t, stat2)

	t.Run("statRegular", func(t *testing.T) {
		stat1, err := s.Stat(path.Join(s.workDir + fname))
		assert.NoError(t, err)
		assert.True(t, stat1.Mode.IsRead())

		size, ok := stat1.GetContentLength()
		assert.True(t, ok)
		assert.Equal(t, size, stat2.Size())

		time, ok := stat1.GetLastModified()
		assert.True(t, ok)
		assert.Equal(t, time, stat2.ModTime())
	})
}

func TestStorage_Read(t *testing.T) {
	s := NewClient(t)

	fname := "TestFileSeek.txt"

	err := s.hdfs.MkdirAll(s.workDir, 0666)
	assert.NoError(t, err)

	f1, err := s.hdfs.Create(path.Join(s.workDir + fname))
	assert.NoError(t, err)

	n, err := f1.Write([]byte("storager"))
	assert.NoError(t, err)
	assert.EqualValues(t, 8, n)

	err = f1.Close()
	assert.NoError(t, err)

	t.Run("validSeek", func(t *testing.T) {
		buf := new(bytes.Buffer)
		o, err := s.Read(path.Join(s.workDir+fname), buf,
			ps.WithOffset(2),
		)
		assert.NoError(t, err)
		assert.EqualValues(t, 6, o)
		assert.Equal(t, "orager", string(buf.Bytes()))

	})

	t.Run("invalidSeek", func(t *testing.T) {
		buf := new(bytes.Buffer)
		o, err := s.Read(path.Join(s.workDir+fname), buf,
			ps.WithOffset(8),
		)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, o)
		assert.Equal(t, "", string(buf.Bytes()))
	})

	t.Run("validSeekSet", func(t *testing.T) {
		buf := new(bytes.Buffer)
		o, err := s.Read(path.Join(s.workDir+fname), buf,
			ps.WithOffset(2),
			ps.WithSize(5),
		)
		assert.NoError(t, err)
		assert.EqualValues(t, 5, o)
		assert.Equal(t, "orage", string(buf.Bytes()))
	})

	t.Run("invalidSeekSet", func(t *testing.T) {
		buf := new(bytes.Buffer)
		o, err := s.Read(path.Join(s.workDir+fname), buf,
			ps.WithOffset(2),
			ps.WithSize(8),
		)
		assert.EqualValues(t, s.formatError("read", io.EOF, path.Join(s.workDir+fname)), err)
		assert.EqualValues(t, 6, o)
		assert.Equal(t, "orager", string(buf.Bytes()))
	})

	t.Run("readWithCallBack", func(t *testing.T) {
		buf := new(bytes.Buffer)

		cur := int64(0)
		fn := func(b []byte) {
			cur += int64(len(b))
		}

		o, err := s.Read(path.Join(s.workDir+fname), buf, ps.WithIoCallback(fn))
		assert.NoError(t, err)
		assert.EqualValues(t, 8, o)
		assert.Equal(t, "storager", string(buf.Bytes()))
	})
}
