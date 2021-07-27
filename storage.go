package hdfs

import (
	"context"
	"errors"
	"github.com/beyondstorage/go-storage/v4/pkg/iowrap"
	"github.com/beyondstorage/go-storage/v4/services"
	"io"
	"os"
	path2 "path"

	. "github.com/beyondstorage/go-storage/v4/types"
)

// ref: [hdfs#Client.Stat](https://pkg.go.dev/github.com/colinmarc/hdfs#Client.Stat)
// Stat returns an os.FileInfo describing the named file or directory.
// But FileInfo.Size don't return a correct write byte size
// So here a pointer of type int64 is used for marking
var Size *int64

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	rp := s.getAbsPath(path)
	o.ID = rp
	o.Path = path
	return o
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)
	err = s.hdfs.Remove(rp)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		// Omit `file not exist` error here
		// ref: [GSP-46](https://github.com/beyondstorage/specs/blob/master/rfcs/46-idempotent-delete.md)
		err = nil
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	rp := s.getAbsPath(path)
	if !opt.HasListMode || opt.ListMode.IsDir() {
		nextFn := func(ctx context.Context, page *ObjectPage) error {
			dir, err := s.hdfs.ReadDir(rp)
			if err != nil {
				return err
			}
			for _, f := range dir {
				o := NewObject(s, true)
				o.Path = s.getFileName(f.Name())
				if f.IsDir() {
					o.Mode |= ModeDir
				} else {
					o.Mode |= ModeRead
				}

				o.SetContentLength(int64(f.Size()))
				page.Data = append(page.Data, o)
			}
			return IterateDone
		}
		oi = NewObjectIterator(ctx, nextFn, nil)
		return
	} else {
		return nil, services.ListModeInvalidError{Actual: opt.ListMode}
	}
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.WorkDir = s.workDir
	return meta
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	rp := s.getAbsPath(path)
	file, err := s.hdfs.Open(rp)
	if err != nil {
		return 0, err
	}
	if opt.HasOffset {
		_, err = file.Seek(opt.Offset, 0)
		if err != nil {
			return 0, err
		}
	}

	var rc io.Reader
	rc = file

	if opt.HasIoCallback {
		rc = iowrap.CallbackReader(rc, opt.IoCallback)
	}

	return io.Copy(w, rc)
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {

	rp := s.getAbsPath(path)

	stat, err := s.hdfs.Stat(rp)
	if err != nil {
		return nil, err
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path

	if stat.IsDir() {
		o.Mode |= ModeDir
		return
	}

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		o.Mode |= ModeDir
	} else {
		o.Mode |= ModeRead
	}

	o.SetContentLength(*Size)

	var sm ObjectSystemMetadata

	sm.Name = stat.Name()
	sm.Size = stat.Size()
	sm.Mode = (uint32)(stat.Mode())
	sm.ModTime = stat.ModTime()
	sm.IsDir = stat.IsDir()
	sm.Sys = stat.Sys()

	o.SetSystemMetadata(sm)

	return o, nil
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	rp := s.getAbsPath(path)

	filename := s.getFileName(path)

	dir := path2.Dir(rp)
	err = s.hdfs.MkdirAll(dir, 0666)
	if err != nil {
		return 0, err
	}
	file := dir + "/" + filename

	var f io.Writer

	_, err = s.hdfs.Stat(file)
	if os.IsNotExist(err) {
		filewrite, err := s.hdfs.Create(file)
		if err != nil {
			return 0, err
		}
		defer filewrite.Close()

		f = filewrite
		if opt.HasIoCallback {
			r = iowrap.CallbackReader(r, opt.IoCallback)
		}

		Size = &size
		filewrite.Flush()
		return io.CopyN(f, r, size)
	} else {
		err = s.hdfs.Remove(rp)
		if err != nil && errors.Is(err, os.ErrNotExist) {
			// Omit `file not exist` error here
			// ref: [GSP-46](https://github.com/beyondstorage/specs/blob/master/rfcs/46-idempotent-delete.md)
			err = nil
		}
		filewrite, err := s.hdfs.Create(file)
		if err != nil {
			return 0, err
		}
		defer filewrite.Close()
		f = filewrite
		if opt.HasIoCallback {
			r = iowrap.CallbackReader(r, opt.IoCallback)
		}
		if err != nil {
			return 0, err
		}
		Size = &size
		filewrite.Flush()
		return io.CopyN(f, r, size)
	}
}
