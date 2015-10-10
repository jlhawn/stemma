package stemma

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type objectWriter struct {
	r            *Repository
	multiWriter  io.Writer
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

	return &objectWriter{
		r:           r,
		multiWriter: io.MultiWriter(tempFile, digester),
		tempFile:    tempFile,
		digester:    digester,
		objectType:  objectType,
	}, nil
}

func (w *objectWriter) Write(p []byte) (n int, err error) {
	n, err = w.multiWriter.Write(p)
	w.bytesWritten += uint64(n)
	return
}

func (w *objectWriter) Digest() Digest {
	return w.digester.Digest()
}

func (w *objectWriter) Commit() (d Descriptor, err error) {
	defer func() {
		if err != nil {
			w.Cancel()
		}
	}()

	if err := w.tempFile.Close(); err != nil {
		return nil, fmt.Errorf("unable to close temporary file: %s", err)
	}

	digest := w.digester.Digest()
	objectPath := w.r.getObjectPath(digest)

	_, err = os.Lstat(objectPath)
	switch {
	case err == nil:
		// An object with this digest already exists.
		if err := os.Remove(w.tempFile.Name()); err != nil {
			return nil, fmt.Errorf("unable to remove temporary file: %s", err)
		}
	case os.IsNotExist(err):
		// Create the object directory if it doesn't already exist.
		objectDir := filepath.Dir(objectPath)
		if err := os.MkdirAll(objectDir, os.FileMode(0755)); err != nil {
			return nil, fmt.Errorf("unable to make object directory: %s", err)
		}

		// Move the object file into place.
		if err := os.Rename(w.tempFile.Name(), objectPath); err != nil {
			return nil, fmt.Errorf("unable to move object into place: %s", err)
		}
	default:
		// Some other error.
		return nil, fmt.Errorf("unable to stat object path: %s", err)
	}

	return &descriptor{
		digest:     digest,
		size:       w.bytesWritten,
		objectType: w.objectType,
	}, nil
}

func (w *objectWriter) Cancel() error {
	w.tempFile.Close()
	return os.Remove(w.tempFile.Name())
}

// GetFile opens the file object with the given digest from this repository.
func (r *Repository) GetFile(digest Digest) (io.ReadCloser, error) {
	return r.getObjectFile(digest)
}

// NewFileWriter begins the process of writing a new file in this repository.
func (r *Repository) NewFileWriter() (FileWriter, error) {
	return r.newObjectWriter(ObjectTypeFile)
}
