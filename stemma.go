package stemma

import (
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

// ObjectStore is the interface for managing content-addressable objects.
type ObjectStore interface {
	// Get the header object with the given digest from this store.
	GetHeader(digest Digest) (Header, error)
	// Put the given header into this object store.
	PutHeader(header Header) (Descriptor, error)
	// Get the file object with the given digest from this store.
	GetFile(digest Digest) (io.ReadCloser, error)
	// NewWriter begins the process of writing a new file using this store.
	NewFileWriter() (FileWriter, error)
	// Get the directory with the given digest from this store.
	GetDirectory(digest Digest) (Directory, error)
	// NewWriter begins the process of writing a new directory using this
	// store.
	NewDirectoryWriter(estimatedSize uint) (DirectoryWriter, error)
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
	Get(tag string) (digest Digest, err error)
	Set(tag, digest Digest) error
	List() (map[string]Digest, error)
	Remove(tag string) error
}

// MountSet is the interface for managing mounts of filesystem directories.
type MountSet interface {
	List() (digests []Digest, err error)
	Add(digest Digest) error
	Remove(digest Digest) error
}
