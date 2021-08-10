package hdfs

import (
	"context"
	"io"

	"github.com/beyondstorage/go-storage/v4/types"

	"github.com/colinmarc/hdfs/v2"
)

const defaultListObjectLimit = 100

type listDirInput struct {
	rp  string
	dir *hdfs.FileReader

	continuationToken string
}

func (i *listDirInput) ContinuationToken() string {
	return i.continuationToken
}

func (s *Storage) listDirNext(ctx context.Context, page *types.ObjectPage) (err error) {
	input := page.Status.(*listDirInput)

	if input.dir == nil {
		input.dir, err = s.hdfs.Open(input.rp)
		if err != nil {
			return
		}
	}

	fileList, err := input.dir.Readdir(defaultListObjectLimit)

	if err != nil && err == io.EOF {
		_ = input.dir.Close()
		input.dir = nil
		return types.IterateDone
	}

	for _, f := range fileList {
		o := s.newObject(true)
		o.ID = input.rp
		o.Path = f.Name()

		if f.Mode().IsDir() {
			o.Mode |= types.ModeDir
		}

		if f.Mode().IsRegular() {
			o.Mode |= types.ModeRead
		}

		o.SetContentLength(f.Size())

		page.Data = append(page.Data, o)
		input.continuationToken = o.Path
	}

	return
}
