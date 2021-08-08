package hdfs

import (
	"context"
	"github.com/beyondstorage/go-storage/v4/types"
	"os"
)

type listDirInput struct {
	rp  string
	dir string

	fileList []os.FileInfo

	continuationToken string
	counter           int
}

func (i *listDirInput) ContinuationToken() string {
	return i.continuationToken
}

func (s *Storage) listDirNext(ctx context.Context, page *types.ObjectPage) (err error) {
	input := page.Status.(*listDirInput)

	input.fileList, err = s.hdfs.ReadDir(input.rp)
	if err != nil {
		return err
	}

	n := len(input.fileList)
	if n <= 0 {
		return types.IterateDone
	}

	for i := 0; i < n; i++ {
		f := input.fileList[i]

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
		input.counter++
	}
	return nil
}
