// Code generated by go generate via cmd/definitions; DO NOT EDIT.
package hdfs

import (
	"context"
	"io"

	"github.com/beyondstorage/go-storage/v4/pkg/credential"
	"github.com/beyondstorage/go-storage/v4/pkg/endpoint"
	"github.com/beyondstorage/go-storage/v4/pkg/httpclient"
	"github.com/beyondstorage/go-storage/v4/services"
	. "github.com/beyondstorage/go-storage/v4/types"
)

var _ credential.Provider
var _ endpoint.Value
var _ Storager
var _ services.ServiceError
var _ httpclient.Options

// Type is the type for hdfs
const Type = "hdfs"

// Service available pairs.
const ()

// ObjectMetadata stores service metadata for object.
type ObjectMetadata struct {
}

// GetObjectMetadata will get ObjectMetadata from Object.
//
// - This function should not be called by service implementer.
// - The returning ObjectMetadata is read only and should not be modified.
func GetObjectMetadata(o *Object) ObjectMetadata {
	om, ok := o.GetServiceMetadata()
	if ok {
		return om.(ObjectMetadata)
	}
	return ObjectMetadata{}
}

// setObjectMetadata will set ObjectMetadata into Object.
//
// - This function should only be called once, please make sure all data has been written before set.
func setObjectMetadata(o *Object, om ObjectMetadata) {
	o.SetServiceMetadata(om)
}

var (
	_ Storager = &Storage{}
)

type StorageFeatures struct {
	LooseOperationAll      bool
	LooseOperationCreate   bool
	LooseOperationDelete   bool
	LooseOperationList     bool
	LooseOperationMetadata bool
	LooseOperationRead     bool
	LooseOperationStat     bool
	LooseOperationWrite    bool

	VirtualOperationAll bool

	VirtualPairAll bool
}

// pairStorageNew is the parsed struct
type pairStorageNew struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
}

// parsePairStorageNew will parse Pair slice into *pairStorageNew
func parsePairStorageNew(opts []Pair) (pairStorageNew, error) {
	result := pairStorageNew{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		}
	}

	return result, nil
}

// DefaultStoragePairs is default pairs for specific action
type DefaultStoragePairs struct {
	Create   []Pair
	Delete   []Pair
	List     []Pair
	Metadata []Pair
	Read     []Pair
	Stat     []Pair
	Write    []Pair
}

// pairStorageCreate is the parsed struct
type pairStorageCreate struct {
	pairs []Pair
}

// parsePairStorageCreate will parse Pair slice into *pairStorageCreate
func (s *Storage) parsePairStorageCreate(opts []Pair) (pairStorageCreate, error) {
	result := pairStorageCreate{
		pairs: opts,
	}

	for _, v := range opts {
		// isUnsupportedPair records whether current pair is unsupported.
		isUnsupportedPair := false

		switch v.Key {
		default:
			isUnsupportedPair = true
		}

		if !isUnsupportedPair {
			continue
		}

		// If user enables the loose operation feature, we will ignore PairUnsupportedError.
		if s.features.LooseOperationAll || s.features.LooseOperationCreate {
			continue
		}
		return pairStorageCreate{}, services.PairUnsupportedError{Pair: v}
	}

	// Check required pairs.

	return result, nil
}

// pairStorageDelete is the parsed struct
type pairStorageDelete struct {
	pairs []Pair
}

// parsePairStorageDelete will parse Pair slice into *pairStorageDelete
func (s *Storage) parsePairStorageDelete(opts []Pair) (pairStorageDelete, error) {
	result := pairStorageDelete{
		pairs: opts,
	}

	for _, v := range opts {
		// isUnsupportedPair records whether current pair is unsupported.
		isUnsupportedPair := false

		switch v.Key {
		default:
			isUnsupportedPair = true
		}

		if !isUnsupportedPair {
			continue
		}

		// If user enables the loose operation feature, we will ignore PairUnsupportedError.
		if s.features.LooseOperationAll || s.features.LooseOperationDelete {
			continue
		}
		return pairStorageDelete{}, services.PairUnsupportedError{Pair: v}
	}

	// Check required pairs.

	return result, nil
}

// pairStorageList is the parsed struct
type pairStorageList struct {
	pairs       []Pair
	HasListMode bool
	ListMode    ListMode
}

// parsePairStorageList will parse Pair slice into *pairStorageList
func (s *Storage) parsePairStorageList(opts []Pair) (pairStorageList, error) {
	result := pairStorageList{
		pairs: opts,
	}

	for _, v := range opts {
		// isUnsupportedPair records whether current pair is unsupported.
		isUnsupportedPair := false

		switch v.Key {
		case "list_mode":
			if result.HasListMode {
				continue
			}
			result.HasListMode = true
			result.ListMode = v.Value.(ListMode)
			continue
		default:
			isUnsupportedPair = true
		}

		if !isUnsupportedPair {
			continue
		}

		// If user enables the loose operation feature, we will ignore PairUnsupportedError.
		if s.features.LooseOperationAll || s.features.LooseOperationList {
			continue
		}
		return pairStorageList{}, services.PairUnsupportedError{Pair: v}
	}

	// Check required pairs.

	return result, nil
}

// pairStorageMetadata is the parsed struct
type pairStorageMetadata struct {
	pairs []Pair
}

// parsePairStorageMetadata will parse Pair slice into *pairStorageMetadata
func (s *Storage) parsePairStorageMetadata(opts []Pair) (pairStorageMetadata, error) {
	result := pairStorageMetadata{
		pairs: opts,
	}

	for _, v := range opts {
		// isUnsupportedPair records whether current pair is unsupported.
		isUnsupportedPair := false

		switch v.Key {
		default:
			isUnsupportedPair = true
		}

		if !isUnsupportedPair {
			continue
		}

		// If user enables the loose operation feature, we will ignore PairUnsupportedError.
		if s.features.LooseOperationAll || s.features.LooseOperationMetadata {
			continue
		}
		return pairStorageMetadata{}, services.PairUnsupportedError{Pair: v}
	}

	// Check required pairs.

	return result, nil
}

// pairStorageRead is the parsed struct
type pairStorageRead struct {
	pairs         []Pair
	HasIoCallback bool
	IoCallback    func([]byte)
	HasOffset     bool
	Offset        int64
	HasSize       bool
	Size          int64
}

// parsePairStorageRead will parse Pair slice into *pairStorageRead
func (s *Storage) parsePairStorageRead(opts []Pair) (pairStorageRead, error) {
	result := pairStorageRead{
		pairs: opts,
	}

	for _, v := range opts {
		// isUnsupportedPair records whether current pair is unsupported.
		isUnsupportedPair := false

		switch v.Key {
		case "io_callback":
			if result.HasIoCallback {
				continue
			}
			result.HasIoCallback = true
			result.IoCallback = v.Value.(func([]byte))
			continue
		case "offset":
			if result.HasOffset {
				continue
			}
			result.HasOffset = true
			result.Offset = v.Value.(int64)
			continue
		case "size":
			if result.HasSize {
				continue
			}
			result.HasSize = true
			result.Size = v.Value.(int64)
			continue
		default:
			isUnsupportedPair = true
		}

		if !isUnsupportedPair {
			continue
		}

		// If user enables the loose operation feature, we will ignore PairUnsupportedError.
		if s.features.LooseOperationAll || s.features.LooseOperationRead {
			continue
		}
		return pairStorageRead{}, services.PairUnsupportedError{Pair: v}
	}

	// Check required pairs.

	return result, nil
}

// pairStorageStat is the parsed struct
type pairStorageStat struct {
	pairs []Pair
}

// parsePairStorageStat will parse Pair slice into *pairStorageStat
func (s *Storage) parsePairStorageStat(opts []Pair) (pairStorageStat, error) {
	result := pairStorageStat{
		pairs: opts,
	}

	for _, v := range opts {
		// isUnsupportedPair records whether current pair is unsupported.
		isUnsupportedPair := false

		switch v.Key {
		default:
			isUnsupportedPair = true
		}

		if !isUnsupportedPair {
			continue
		}

		// If user enables the loose operation feature, we will ignore PairUnsupportedError.
		if s.features.LooseOperationAll || s.features.LooseOperationStat {
			continue
		}
		return pairStorageStat{}, services.PairUnsupportedError{Pair: v}
	}

	// Check required pairs.

	return result, nil
}

// pairStorageWrite is the parsed struct
type pairStorageWrite struct {
	pairs          []Pair
	HasContentMd5  bool
	ContentMd5     string
	HasContentType bool
	ContentType    string
	HasIoCallback  bool
	IoCallback     func([]byte)
}

// parsePairStorageWrite will parse Pair slice into *pairStorageWrite
func (s *Storage) parsePairStorageWrite(opts []Pair) (pairStorageWrite, error) {
	result := pairStorageWrite{
		pairs: opts,
	}

	for _, v := range opts {
		// isUnsupportedPair records whether current pair is unsupported.
		isUnsupportedPair := false

		switch v.Key {
		case "content_md5":
			if result.HasContentMd5 {
				continue
			}
			result.HasContentMd5 = true
			result.ContentMd5 = v.Value.(string)
			continue
		case "content_type":
			if result.HasContentType {
				continue
			}
			result.HasContentType = true
			result.ContentType = v.Value.(string)
			continue
		case "io_callback":
			if result.HasIoCallback {
				continue
			}
			result.HasIoCallback = true
			result.IoCallback = v.Value.(func([]byte))
			continue
		default:
			isUnsupportedPair = true
		}

		if !isUnsupportedPair {
			continue
		}

		// If user enables the loose operation feature, we will ignore PairUnsupportedError.
		if s.features.LooseOperationAll || s.features.LooseOperationWrite {
			continue
		}
		return pairStorageWrite{}, services.PairUnsupportedError{Pair: v}
	}

	// Check required pairs.

	return result, nil
}

// Create will create a new object without any api call.
//
// This function will create a context by default.
func (s *Storage) Create(path string, pairs ...Pair) (o *Object) {
	pairs = append(pairs, s.defaultPairs.Create...)
	var opt pairStorageCreate

	// Ignore error while handling local funtions.
	opt, _ = s.parsePairStorageCreate(pairs)

	return s.create(path, opt)
}

// Delete will delete an Object from service.
//
// This function will create a context by default.
func (s *Storage) Delete(path string, pairs ...Pair) (err error) {
	ctx := context.Background()
	return s.DeleteWithContext(ctx, path, pairs...)
}

// DeleteWithContext will delete an Object from service.
func (s *Storage) DeleteWithContext(ctx context.Context, path string, pairs ...Pair) (err error) {
	defer func() {
		err = s.formatError("delete", err, path)
	}()

	pairs = append(pairs, s.defaultPairs.Delete...)
	var opt pairStorageDelete

	opt, err = s.parsePairStorageDelete(pairs)
	if err != nil {
		return
	}

	return s.delete(ctx, path, opt)
}

// List will return list a specific path.
//
// This function will create a context by default.
func (s *Storage) List(path string, pairs ...Pair) (oi *ObjectIterator, err error) {
	ctx := context.Background()
	return s.ListWithContext(ctx, path, pairs...)
}

// ListWithContext will return list a specific path.
func (s *Storage) ListWithContext(ctx context.Context, path string, pairs ...Pair) (oi *ObjectIterator, err error) {
	defer func() {
		err = s.formatError("list", err, path)
	}()

	pairs = append(pairs, s.defaultPairs.List...)
	var opt pairStorageList

	opt, err = s.parsePairStorageList(pairs)
	if err != nil {
		return
	}

	return s.list(ctx, path, opt)
}

// Metadata will return current storager metadata.
//
// This function will create a context by default.
func (s *Storage) Metadata(pairs ...Pair) (meta *StorageMeta) {
	pairs = append(pairs, s.defaultPairs.Metadata...)
	var opt pairStorageMetadata

	// Ignore error while handling local funtions.
	opt, _ = s.parsePairStorageMetadata(pairs)

	return s.metadata(opt)
}

// Read will read the file's data.
//
// This function will create a context by default.
func (s *Storage) Read(path string, w io.Writer, pairs ...Pair) (n int64, err error) {
	ctx := context.Background()
	return s.ReadWithContext(ctx, path, w, pairs...)
}

// ReadWithContext will read the file's data.
func (s *Storage) ReadWithContext(ctx context.Context, path string, w io.Writer, pairs ...Pair) (n int64, err error) {
	defer func() {
		err = s.formatError("read", err, path)
	}()

	pairs = append(pairs, s.defaultPairs.Read...)
	var opt pairStorageRead

	opt, err = s.parsePairStorageRead(pairs)
	if err != nil {
		return
	}

	return s.read(ctx, path, w, opt)
}

// Stat will stat a path to get info of an object.
//
// This function will create a context by default.
func (s *Storage) Stat(path string, pairs ...Pair) (o *Object, err error) {
	ctx := context.Background()
	return s.StatWithContext(ctx, path, pairs...)
}

// StatWithContext will stat a path to get info of an object.
func (s *Storage) StatWithContext(ctx context.Context, path string, pairs ...Pair) (o *Object, err error) {
	defer func() {
		err = s.formatError("stat", err, path)
	}()

	pairs = append(pairs, s.defaultPairs.Stat...)
	var opt pairStorageStat

	opt, err = s.parsePairStorageStat(pairs)
	if err != nil {
		return
	}

	return s.stat(ctx, path, opt)
}

// Write will write data into a file.
//
// This function will create a context by default.
func (s *Storage) Write(path string, r io.Reader, size int64, pairs ...Pair) (n int64, err error) {
	ctx := context.Background()
	return s.WriteWithContext(ctx, path, r, size, pairs...)
}

// WriteWithContext will write data into a file.
func (s *Storage) WriteWithContext(ctx context.Context, path string, r io.Reader, size int64, pairs ...Pair) (n int64, err error) {
	defer func() {
		err = s.formatError("write", err, path)
	}()

	pairs = append(pairs, s.defaultPairs.Write...)
	var opt pairStorageWrite

	opt, err = s.parsePairStorageWrite(pairs)
	if err != nil {
		return
	}

	return s.write(ctx, path, r, size, opt)
}

func init() {
	services.RegisterStorager(Type, NewStorager)
}
