package stemma

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
