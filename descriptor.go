package stemma

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
)

// Descriptor describes an object.
type Descriptor interface {
	Digest() Digest
	Size() uint64
	Type() ObjectType

	// If this object is a directory, NumSubObjects is the total number
	// of objects references by the directory and all subdirectories and
	// SubObjectsSize is the total size of all of the referenced objects.
	NumSubObjects() uint32
	SubObjectsSize() uint64
}

type descriptor struct {
	digest     Digest
	size       uint64
	objectType ObjectType

	numSubObjects  uint32
	subObjectsSize uint64
}

func (d *descriptor) Digest() Digest {
	return d.digest
}

func (d *descriptor) Size() uint64 {
	return d.size
}

func (d *descriptor) Type() ObjectType {
	return d.objectType
}

func (d *descriptor) NumSubObjects() uint32 {
	return d.numSubObjects
}

func (d *descriptor) SubObjectsSize() uint64 {
	return d.subObjectsSize
}

// marshal marshals this descriptor to its binary encoding to the given writer.
func MarshalDescriptor(w io.Writer, d Descriptor) error {
	if err := d.Digest().Marshal(w); err != nil {
		return fmt.Errorf("unable to encode object digest: %s", err)
	}

	if err := binary.Write(w, binary.LittleEndian, d.Size()); err != nil {
		return fmt.Errorf("unable to encode object size: %s", err)
	}

	if _, err := w.Write([]byte{byte(d.Type())}); err != nil {
		return fmt.Errorf("unable to encode object type: %s", err)
	}

	if err := binary.Write(w, binary.LittleEndian, d.NumSubObjects()); err != nil {
		return fmt.Errorf("unable to encode subobject count: %s", err)
	}

	if err := binary.Write(w, binary.LittleEndian, d.SubObjectsSize()); err != nil {
		return fmt.Errorf("unable to encode total subobject size: %s", err)
	}

	return nil
}

// unmarshalDescriptor unmarshals a descriptor object from its binary encoding
// from the given reader.
func UnmarshalDescriptor(r io.Reader) (Descriptor, error) {
	var (
		d   descriptor
		err error
	)

	if d.digest, err = UnmarshalDigest(r); err != nil {
		return nil, fmt.Errorf("unable to decode object digest: %s", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &d.size); err != nil {
		return nil, fmt.Errorf("unable to decode object size: %s", err)
	}

	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return nil, fmt.Errorf("unable to decode object type: %s", err)
	}
	d.objectType = ObjectType(typeBuf[0])

	if err := binary.Read(r, binary.LittleEndian, &d.numSubObjects); err != nil {
		return nil, fmt.Errorf("unable to decode subobject count: %s", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &d.subObjectsSize); err != nil {
		return nil, fmt.Errorf("unable to decode total subobject size: %s", err)
	}

	return &d, nil
}

// DescriptorQueue is a FIFO queue of object descriptors.
type DescriptorQueue interface {
	Len() int
	Empty() bool
	Full() bool
	PushBack(desc Descriptor)
	Peek() Descriptor
	Pop() Descriptor
}

// DescriptorStack is a LIFO queue of object descriptors.
type DescriptorStack interface {
	Len() int
	Empty() bool
	Full() bool
	PushFront(desc Descriptor)
	Peek() Descriptor
	Pop() Descriptor
}

// descriptorList is a list of object descriptors.
type descriptorList struct {
	list.List
	maxSize int
}

// NewDescriptorList creates a new descriptorList with the given (advisory)
// maximum size. If maxSize <= 0, Full() will always return false.
func newDescriptorList(maxSize int) *descriptorList {
	return &descriptorList{
		List:    list.List{},
		maxSize: maxSize,
	}
}

// NewDescriptorQueue creates a new FIFO queue with the given (advisory)
// maximum size. If maxSize <= 0, Full() will always return false.
func NewDescriptorQueue(maxSize int) DescriptorQueue {
	return newDescriptorList(maxSize)
}

// NewDescriptorStack creates a new LIFO queue with the given (advisory)
// maximum size. If maxSize <= 0, Full() will always return false.
func NewDescriptorStack(maxSize int) DescriptorStack {
	return newDescriptorList(maxSize)
}

func (l *descriptorList) Empty() bool {
	return l.Len() == 0
}

func (l *descriptorList) Full() bool {
	if l.maxSize <= 0 {
		return false
	}

	return l.Len() >= l.maxSize
}

func (l *descriptorList) PushBack(desc Descriptor) {
	l.List.PushBack(desc)
}

func (l *descriptorList) PushFront(desc Descriptor) {
	l.List.PushFront(desc)
}

func (l *descriptorList) Peek() Descriptor {
	return l.Front().Value.(Descriptor)
}

func (l *descriptorList) Pop() Descriptor {
	return l.Remove(l.Front()).(Descriptor)
}
