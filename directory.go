package stemma

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// DirentType specifies the type of an entry in a directory listing.
type DirentType byte

// Available directory entry types.
const (
	DirentTypeUnknown DirentType = iota
	DirentTypeBlockDevice
	DirentTypeCharDevice
	DirentTypeDirectory
	DirentTypeFifo
	DirentTypeLink
	DirentTypeRegular
	DirentTypeSocket
)

// DirectoryEntry provides fields of a directory entry. ObjectSize is stored on
// the directory entry rather than inside the header object for 2 reasons:
// 1) Having the size in the directory entry allows for knowing the expected
// size of the object to fetch which can prevent an endless-data attack.
// 2) Not having the size in the header object (along with not storing
// timestamps in the header object) allows for reuse of the header object when
// only the value of the object has changed and all header fields have remained
// the same.
type DirectoryEntry struct {
	Name string
	Type DirentType

	LinkTarget string // If type is DirentTypeLink.

	HeaderDigest Digest // Digest of the object's corresponding header.
	HeaderSize   uint64 // Size of Header object in Bytes

	ObjectDigest Digest // Digest of the object if regular file or dir.
	ObjectSize   uint64 // Size of Object in Bytes.

	// If this entry is a subdirectory, NumSubObjects is the number of
	// additional objects in the subdirectory (headers + regular files +
	// subdirectories(...)). SubObjectSize is the total size of each
	// subdirectory entry header + object size + recursive subobject size.
	NumSubObjects  uint32
	SubObjectsSize uint64
}

// IsDir returns whether this directory entry is of type DirentTypeDirectory.
func (de DirectoryEntry) IsDir() bool {
	return de.Type == DirentTypeDirectory
}

// HeaderDescriptor returns a descriptor for the header object associated with
// this directory entry.
func (de DirectoryEntry) HeaderDescriptor() Descriptor {
	return &descriptor{
		digest:     de.HeaderDigest,
		size:       de.HeaderSize,
		objectType: ObjectTypeHeader,
	}
}

// ObjectDescriptor returns a descriptor for the file or directory object
// associated with this directory entry. If this directory entry is not a
// regular file or directory object, a nil descriptor is returned.
func (de DirectoryEntry) ObjectDescriptor() Descriptor {
	switch de.Type {
	case DirentTypeDirectory, DirentTypeRegular:
		// Continue.
	default:
		return nil
	}

	objType := ObjectTypeFile
	if de.Type == DirentTypeDirectory {
		objType = ObjectTypeDirectory
	}

	return &descriptor{
		digest:         de.ObjectDigest,
		size:           de.ObjectSize,
		objectType:     objType,
		numSubObjects:  de.NumSubObjects,
		subObjectsSize: de.SubObjectsSize,
	}
}

// Directory is a list of directory entries. Implements sort.Interface.
type Directory []DirectoryEntry

// TotalNumSubOjbects returns the total number of objects referenced by this
// directory and any subdirectories.
func (d Directory) TotalNumSubOjbects() uint32 {
	numObjects := uint32(len(d)) // One Header object for each directory entry.
	for _, de := range d {
		switch de.Type {
		case DirentTypeRegular, DirentTypeDirectory:
			// 1 for the object itself plus all subobjects (if it
			// is a subdirectory).
			numObjects += 1 + de.NumSubObjects
		}
	}

	return numObjects
}

// TotalSubOjbectSize returns the total size of objects referenced by this
// directory and any subdirectories.
func (d Directory) TotalSubOjbectSize() uint64 {
	var objectSize uint64
	for _, de := range d {
		// All entries will have a header. Regular fles and
		// directories will have a referenced object. Directories will
		// have their total subobject size.
		objectSize += de.HeaderSize + de.ObjectSize + de.SubObjectsSize
	}

	return objectSize
}

func (d Directory) Len() int {
	return len(d)
}

func (d Directory) Less(i, j int) bool {
	e1, e2 := d[i], d[j]

	// If either both are directories or both are not directories.
	if e1.IsDir() == e2.IsDir() {
		// Order by name.
		return e1.Name < e2.Name
	}

	// Order directories before other types.
	return e1.IsDir()
}

func (d Directory) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

// DirectoryWriter provides a handle for writing a new directory object into a
// DirectoryStore.
type DirectoryWriter interface {
	Add(entry DirectoryEntry)
	Commit() (Descriptor, error)
}

type directoryWriter struct {
	r         *Repository
	directory Directory
}

// NewDirectoryWriter begins the process of writing a new directory into this
// repository.
func (r *Repository) NewDirectoryWriter(estimatedSize uint) (DirectoryWriter, error) {
	return &directoryWriter{
		r:         r,
		directory: make(Directory, 0, estimatedSize),
	}, nil
}

func (w *directoryWriter) Add(entry DirectoryEntry) {
	w.directory = append(w.directory, entry)
}

func (w *directoryWriter) Commit() (Descriptor, error) {
	objectWriter, err := w.r.newObjectWriter(ObjectTypeDirectory)
	if err != nil {
		return nil, fmt.Errorf("unable to get new object writer: %s", err)
	}

	sort.Sort(w.directory)

	if err := w.directory.Marshal(objectWriter); err != nil {
		objectWriter.Cancel()
		return nil, fmt.Errorf("unable to encode directory object: %s", err)
	}

	desc, err := objectWriter.Commit()
	if err != nil {
		return nil, fmt.Errorf("unable to commit directory object: %s", err)
	}

	return &descriptor{
		digest:         desc.Digest(),
		size:           desc.Size(),
		objectType:     desc.Type(),
		numSubObjects:  w.directory.TotalNumSubOjbects(),
		subObjectsSize: w.directory.TotalSubOjbectSize(),
	}, nil
}

// GetDirectory gets the directory with the given digest from this repository.
func (r *Repository) GetDirectory(digest Digest) (Directory, error) {
	object, err := r.getObjectFile(digest)
	if err != nil {
		return nil, fmt.Errorf("unable to get directory object: %s", err)
	}

	defer object.Close()

	return UnmarshalDirectory(object)
}

// Marshal marshals the binary encoding (little-endian) of this directory into
// the given writer.
func (d Directory) Marshal(w io.Writer) error {
	numEntries := uint32(len(d))
	if err := binary.Write(w, binary.LittleEndian, numEntries); err != nil {
		return fmt.Errorf("unable to encode length of directory: %s", err)
	}

	for _, entry := range d {
		if err := entry.Marshal(w); err != nil {
			return fmt.Errorf("unable to encode directory entry: %s", err)
		}
	}

	return nil
}

// UnmarshalDirectory unmarshals the binary encoding (little-endian) of a
// directory from the given reader.
func UnmarshalDirectory(r io.Reader) (d Directory, err error) {
	var numEntries uint32
	if err := binary.Read(r, binary.LittleEndian, &numEntries); err != nil {
		return nil, fmt.Errorf("unable to decode length of directory: %s", err)
	}

	d = make(Directory, numEntries)
	for i := range d {
		if d[i], err = UnmarshalDirectoryEntry(r); err != nil {
			return nil, fmt.Errorf("unable to decode directory entry: %s", err)
		}
	}

	return d, nil
}

// Marshal marshals the binary encoding (little-endian) of this directory entry
// into the given writer.
func (de DirectoryEntry) Marshal(w io.Writer) error {
	// Write the entry name.
	if err := marshalBytes(w, []byte(de.Name)); err != nil {
		return fmt.Errorf("unable to encode directory entry name: %s", err)
	}

	// Write the directory entry type (1 byte).
	if _, err := w.Write([]byte{byte(de.Type)}); err != nil {
		return fmt.Errorf("unable to encode directory entry type: %s", err)
	}

	// Write the link target.
	if err := marshalBytes(w, []byte(de.LinkTarget)); err != nil {
		return fmt.Errorf("unable to encode directory entry link target: %s", err)
	}

	// Write the header digest.
	if err := de.HeaderDigest.Marshal(w); err != nil {
		return fmt.Errorf("unable to encode directory entry header digest: %s", err)
	}

	// Write the header object size.
	if err := binary.Write(w, binary.LittleEndian, de.HeaderSize); err != nil {
		return fmt.Errorf("unable to encode directory entry header object size: %s", err)
	}

	// Write the object digest.
	if err := de.ObjectDigest.Marshal(w); err != nil {
		return fmt.Errorf("unable to encode directory entry object digest: %s", err)
	}

	// Write the entry object size.
	if err := binary.Write(w, binary.LittleEndian, de.ObjectSize); err != nil {
		return fmt.Errorf("unable to encode directory entry object size: %s", err)
	}

	// Write the number of subobjects.
	if err := binary.Write(w, binary.LittleEndian, de.NumSubObjects); err != nil {
		return fmt.Errorf("unable to encode number of directory entry subobjects: %s", err)
	}

	// Write the total subobject size.
	if err := binary.Write(w, binary.LittleEndian, de.SubObjectsSize); err != nil {
		return fmt.Errorf("unable to encode size of directory entry subobjects: %s", err)
	}

	return nil
}

// UnmarshalDirectoryEntry unmarshals the binary encoding (little-endian) of a
// directory entry from the given reader.
func UnmarshalDirectoryEntry(r io.Reader) (de DirectoryEntry, err error) {
	// Write the entry name.
	nameBuf, err := unmarshalBytes(r)
	if err != nil {
		return de, fmt.Errorf("unable to decode directory entry name: %s", err)
	}
	de.Name = string(nameBuf)

	// Read the directory entry type (1 byte).
	typeBuf := []byte{0}
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return de, fmt.Errorf("unable to decode directory entry type: %s", err)
	}
	de.Type = DirentType(typeBuf[0])

	// Read the link target.
	linkTargetBuf, err := unmarshalBytes(r)
	if err != nil {
		return de, fmt.Errorf("unable to decode directory entry link target: %s", err)
	}
	de.LinkTarget = string(linkTargetBuf)

	// Read the header digest.
	if de.HeaderDigest, err = UnmarshalDigest(r); err != nil {
		return de, fmt.Errorf("unable to decode directory entry header object digest: %s", err)
	}

	// Write the header object size.
	if err := binary.Read(r, binary.LittleEndian, &de.HeaderSize); err != nil {
		return de, fmt.Errorf("unable to decode directory entry header object size: %s", err)
	}

	// Read the object digest.
	if de.ObjectDigest, err = UnmarshalDigest(r); err != nil {
		return de, fmt.Errorf("unable to decode directory entry object digest: %s", err)
	}

	// Read the entry object size.
	if err := binary.Read(r, binary.LittleEndian, &de.ObjectSize); err != nil {
		return de, fmt.Errorf("unable to decode directory entry object size: %s", err)
	}

	// Read the number of subobjects.
	if err := binary.Read(r, binary.LittleEndian, &de.NumSubObjects); err != nil {
		return de, fmt.Errorf("unable to decode number of directory entry subojects: %s", err)
	}

	// Read the total subobject size.
	if err := binary.Read(r, binary.LittleEndian, &de.SubObjectsSize); err != nil {
		return de, fmt.Errorf("unable to decode size of directory entry subobjects: %s", err)
	}

	return de, nil
}
