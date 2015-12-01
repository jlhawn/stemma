package stemma

import (
	"fmt"
	"io"
)

// ObjectType is used to indicate the type of object which is stored.
type ObjectType byte

// Various object types.
const (
	ObjectTypeFile ObjectType = iota
	ObjectTypeDirectory
	ObjectTypeHeader
	ObjectTypeApplication
)

// EncodedObjectTypeSize is the size in bytes for an encoded object type which
// is placed at the begining of all object files.
const EncodedObjectTypeSize = 1

func (ot ObjectType) String() string {
	switch ot {
	case ObjectTypeFile:
		return "file"
	case ObjectTypeDirectory:
		return "directory"
	case ObjectTypeHeader:
		return "header"
	case ObjectTypeApplication:
		return "application"
	default:
		return "unknown"
	}
}

// Marshal writes this object type as a single byte header for objects using
// the given writer.
func (ot ObjectType) Marshal(w io.Writer) error {
	if _, err := w.Write([]byte{byte(ot)}); err != nil {
		return fmt.Errorf("unable to write object type: %s", err)
	}

	return nil
}

// UnmarshalObjectType reads an encoded ObjectType from the given reader.
func UnmarshalObjectType(r io.Reader) (ot ObjectType, err error) {
	buf := make([]byte, 1)
	if _, err := r.Read(buf); err != nil {
		return ot, fmt.Errorf("unable to read object type: %s", err)
	}

	return ObjectType(buf[0]), nil
}

// EnsureObjectType reads from the given reader to unmarshal an object type.
// The decoded object type is checked to ensure it is equal to the expected.
func EnsureObjectType(r io.Reader, expected ObjectType) error {
	actual, err := UnmarshalObjectType(r)
	if err != nil {
		return fmt.Errorf("unable to decode object type: %s", err)
	}

	if actual != expected {
		return fmt.Errorf("invalid object type: expected %s, got %s", expected, actual)
	}

	return nil
}

// ReadSeekCloser is a combination reader, seeker, and closer.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// ObjectStore is the interface for managing content-addressable objects.
type ObjectStore interface {
	// Whether an object with the given digest exists in this store.
	Contains(digest Digest) bool
	// Get the header object with the given digest from this store.
	GetHeader(digest Digest) (Header, error)
	// Put the given header into this object store.
	PutHeader(header Header) (Descriptor, error)
	// Get the file object with the given digest from this store.
	GetFile(digest Digest) (ReadSeekCloser, error)
	// NewWriter begins the process of writing a new file using this store.
	NewFileWriter() (FileWriter, error)
	// Get the directory with the given digest from this store.
	GetDirectory(digest Digest) (Directory, error)
	// NewWriter begins the process of writing a new directory using this
	// store.
	NewDirectoryWriter(estimatedSize uint) (DirectoryWriter, error)
	// Get the application object with the given digest from this store.
	GetApplication(digest Digest) (Application, error)
	// Put the given application into this object store.
	PutApplication(a Application) (Descriptor, error)
}

// FileWriter provides a handle for writing a new file object into a FileStore.
type FileWriter interface {
	io.Writer
	// Digest returns the digest of the data which has been written so far.
	Digest() Digest
	// Commit completes the object writing process, cleaning up any
	// temporary resources. The new object is stored with the given object
	// type and computed digest.
	Commit() (Descriptor, error)
	// Cancel ends the writing process, cleaning up any temporary
	// resources.
	Cancel() error
}

// TagStore is the interface for managing mappings of simple strings to a
// content-addressable filesystem object digest.
type TagStore interface {
	Get(tag string) (Descriptor, error)
	Set(tag string, desc Descriptor) error
	List() (tags []string, err error)
	Remove(tag string) error
}

// MountSet is the interface for managing mounts of application container
// rootfs directories.
type MountSet interface {
	List() (digests []Digest, err error)
	Add(digest Digest) error
	Remove(digest Digest) error
}
