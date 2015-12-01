package stemma

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type objectWriter struct {
	r            *Repository
	buffer       *bufio.Writer
	tempFile     *os.File
	digester     Digester
	objectType   ObjectType
	bytesWritten uint64
}

var _ FileWriter = &objectWriter{}

func (r *Repository) newObjectWriter(objectType ObjectType) (*objectWriter, error) {
	digester, err := NewDigester(DigestAlgSHA512_256)
	if err != nil {
		return nil, fmt.Errorf("unable to create new object digester: %s", err)
	}

	tempFile, err := r.tempFile()
	if err != nil {
		return nil, fmt.Errorf("unable to get temporary object file: %s", err)
	}

	buffer := bufio.NewWriter(io.MultiWriter(tempFile, digester))

	// Write the object type header first. Note: This does not count
	// towards object size.
	if err := objectType.Marshal(buffer); err != nil {
		return nil, err
	}

	return &objectWriter{
		r:          r,
		buffer:     buffer,
		tempFile:   tempFile,
		digester:   digester,
		objectType: objectType,
	}, nil
}

func (w *objectWriter) Write(p []byte) (n int, err error) {
	n, err = w.buffer.Write(p)
	w.bytesWritten += uint64(n)
	return
}

func (w *objectWriter) Flush() error {
	return w.buffer.Flush()
}

func (w *objectWriter) Digest() Digest {
	return w.digester.Digest()
}

func (w *objectWriter) Hold() (tr TempRef, err error) {
	defer func() {
		if err != nil {
			w.Cancel()
		}
	}()

	if err := w.buffer.Flush(); err != nil {
		return nil, fmt.Errorf("unable to flush write buffer: %s", err)
	}

	if err := w.tempFile.Close(); err != nil {
		return nil, fmt.Errorf("unable to close temporary file: %s", err)
	}

	digest := w.Digest()

	return &tempRef{
		desc: &descriptor{
			digest:     digest,
			size:       w.bytesWritten,
			objectType: w.objectType,
		},
		tempPath:        w.tempFile.Name(),
		destinationPath: w.r.getObjectPath(digest),
	}, nil
}

func (w *objectWriter) Commit() (Descriptor, error) {
	tr, err := w.Hold()
	if err != nil {
		return nil, err
	}

	return tr.Commit()
}

func (w *objectWriter) Cancel() error {
	w.tempFile.Close()
	return os.Remove(w.tempFile.Name())
}

// TempRef refers to an object which has been downloaded but not yet commited
// to an object store. This is usually done when an object has been downloaded
// but we are waiting to fetch its dependencies.
type TempRef interface {
	Commit() (Descriptor, error)
	Descriptor() Descriptor
}

type tempRef struct {
	desc            Descriptor
	tempPath        string
	destinationPath string
}

func (tr *tempRef) Descriptor() Descriptor {
	return tr.desc
}

func (tr *tempRef) Commit() (d Descriptor, err error) {
	defer func() {
		if err != nil {
			os.Remove(tr.tempPath)
		}
	}()

	_, err = os.Lstat(tr.destinationPath)
	switch {
	case err == nil:
		// An object with this digest already exists.
		if err := os.Remove(tr.tempPath); err != nil {
			return nil, fmt.Errorf("unable to remove temporary file: %s", err)
		}
	case os.IsNotExist(err):
		// Create the object directory if it doesn't already exist.
		objectDir := filepath.Dir(tr.destinationPath)
		if err := os.MkdirAll(objectDir, os.FileMode(0755)); err != nil {
			return nil, fmt.Errorf("unable to make object directory: %s", err)
		}

		// Move the object file into place.
		if err := os.Rename(tr.tempPath, tr.destinationPath); err != nil {
			return nil, fmt.Errorf("unable to move object into place: %s", err)
		}
	default:
		// Some other error.
		return nil, fmt.Errorf("unable to stat object path: %s", err)
	}

	return tr.desc, nil
}

// offsetSeekWrapper is used to wrap file objects so that seeking never reads
// the first byte object type.
type offsetSeekWrapper struct {
	ReadSeekCloser
	relOffset int64
}

func (osw *offsetSeekWrapper) Seek(offset int64, whence int) (newOffset int64, err error) {
	if whence == os.SEEK_SET {
		// Just add our relative offset.
		return osw.ReadSeekCloser.Seek(offset+osw.relOffset, os.SEEK_SET)
	}

	// We don't know what the undelying file size is so correct the new
	// offset only if necessary.
	newOffset, err = osw.ReadSeekCloser.Seek(offset, whence)
	if err != nil {
		return newOffset, err
	}

	// Ensure that the new offset is greater than or equal to our relative
	// offset.
	if newOffset >= osw.relOffset {
		return newOffset, nil
	}

	// The seek has run past the beginning, go to the relative offset.
	return osw.ReadSeekCloser.Seek(osw.relOffset, os.SEEK_SET)
}

// GetFile opens the file object with the given digest from this repository.
func (r *Repository) GetFile(digest Digest) (ReadSeekCloser, error) {
	object, err := r.getObjectFile(digest)
	if err != nil {
		return nil, fmt.Errorf("unable to get file object: %s", err)
	}

	// The original file contents begin after the object type header.
	if err := EnsureObjectType(object, ObjectTypeFile); err != nil {
		return nil, err
	}

	return &offsetSeekWrapper{
		ReadSeekCloser: object,
		relOffset:      EncodedObjectTypeSize,
	}, nil
}

// NewFileWriter begins the process of writing a new file in this repository.
func (r *Repository) NewFileWriter() (FileWriter, error) {
	return r.newObjectWriter(ObjectTypeFile)
}
