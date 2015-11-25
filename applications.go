package stemma

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Application describes the rootfs and configuration for an application
// container.
type Application struct {
	Rootfs Rootfs
}

// Rootfs describes the rootfs directory+header for an application containier.
type Rootfs struct {
	Header    RootfsHeader
	Directory RootfsDirectory
}

// RootfsHeader describes the rootfs directory header object for an application
// container.
type RootfsHeader struct {
	Digest Digest
	Size   uint64
}

// RootfsDirectory describes the rootfs directory object for an application
// container.
type RootfsDirectory struct {
	Digest         Digest
	Size           uint64
	NumSubObjects  uint32
	SubObjectsSize uint64
}

// GetApplication gets the application object with the given digest from this
// repository.
func (r *Repository) GetApplication(digest Digest) (a Application, err error) {
	object, err := r.getObjectFile(digest)
	if err != nil {
		return a, fmt.Errorf("unable to get application object: %s", err)
	}

	defer object.Close()

	a, err = UnmarshalApplication(object)
	if err != nil {
		return a, fmt.Errorf("unable to decode application object: %s", err)
	}

	return a, nil
}

// PutApplication puts the given application into this object repository.
func (r *Repository) PutApplication(a Application) (Descriptor, error) {
	objectWriter, err := r.newObjectWriter(ObjectTypeApplication)
	if err != nil {
		return nil, fmt.Errorf("unable to get new object writer: %s", err)
	}

	if err := a.Marshal(objectWriter); err != nil {
		objectWriter.Cancel()
		return nil, fmt.Errorf("unable to encode application object: %s", err)
	}

	desc, err := objectWriter.Commit()
	if err != nil {
		return nil, fmt.Errorf("unable to commit application object: %s", err)
	}

	return &descriptor{
		digest:         desc.Digest(),
		size:           desc.Size(),
		objectType:     desc.Type(),
		numSubObjects:  2 + a.Rootfs.Directory.NumSubObjects, // Header, Directory, and Directory subobjects.
		subObjectsSize: a.Rootfs.Header.Size + a.Rootfs.Directory.Size + a.Rootfs.Directory.SubObjectsSize,
	}, nil
}

// Marshal marshals this application to a binary encoding (little-endian) to
// the given writer.
func (a Application) Marshal(w io.Writer) error {
	if err := a.Rootfs.Marshal(w); err != nil {
		return fmt.Errorf("unable to encode rootfs: %s", err)
	}

	return nil
}

// UnmarshalApplication unmarshals an Application from the binary encoding
// (little-endian) read from the given reader.
func UnmarshalApplication(r io.Reader) (a Application, err error) {
	if a.Rootfs, err = UnmarshalRootfs(r); err != nil {
		return a, fmt.Errorf("unable to decode rootfs: %s", err)
	}

	return a, nil
}

// Marshal marshals this Rootfs to a binary encoding (little-endian) to the
// given writer.
func (rfs Rootfs) Marshal(w io.Writer) error {
	// Write the header digest.
	if err := rfs.Header.Digest.Marshal(w); err != nil {
		return fmt.Errorf("unable to encode header digest: %s", err)
	}

	// Write the header size.
	if err := binary.Write(w, binary.LittleEndian, rfs.Header.Size); err != nil {
		return fmt.Errorf("unable to encode header size: %s", err)
	}

	// Write the directory digest.
	if err := rfs.Directory.Digest.Marshal(w); err != nil {
		return fmt.Errorf("unable to encode directory digest: %s", err)
	}

	// Write the directory size.
	if err := binary.Write(w, binary.LittleEndian, rfs.Directory.Size); err != nil {
		return fmt.Errorf("unable to encode directory size: %s", err)
	}

	// Write the directory subobject count.
	if err := binary.Write(w, binary.LittleEndian, rfs.Directory.NumSubObjects); err != nil {
		return fmt.Errorf("unable to encode directory subobject count: %s", err)
	}

	// Write the directory total subobject size.
	if err := binary.Write(w, binary.LittleEndian, rfs.Directory.SubObjectsSize); err != nil {
		return fmt.Errorf("unable to encode directory total subobject size: %s", err)
	}

	return nil
}

// UnmarshalRootfs unbarshals a Rootfs from the binary encoding (littel-endian)
// read from the given reader.
func UnmarshalRootfs(r io.Reader) (rfs Rootfs, err error) {
	// Read the rootfs header digest.
	if rfs.Header.Digest, err = UnmarshalDigest(r); err != nil {
		return rfs, fmt.Errorf("unable to decode rootfs header digest: %s", err)
	}

	// Read the rootfs header size.
	if err := binary.Read(r, binary.LittleEndian, &rfs.Header.Size); err != nil {
		return rfs, fmt.Errorf("unable to decode rootfs header size: %s", err)
	}

	// Read the rootfs directory digest.
	if rfs.Directory.Digest, err = UnmarshalDigest(r); err != nil {
		return rfs, fmt.Errorf("unable to decode rootfs directory digest: %s", err)
	}

	// Read the rootfs directory size.
	if err := binary.Read(r, binary.LittleEndian, &rfs.Directory.Size); err != nil {
		return rfs, fmt.Errorf("unable to decode rootfs directory size: %s", err)
	}

	// Read the rootfs directory subobject count.
	if err := binary.Read(r, binary.LittleEndian, &rfs.Directory.NumSubObjects); err != nil {
		return rfs, fmt.Errorf("unable to decode rootfs directory subobject count: %s", err)
	}

	// Read the rootfs directory total subobject size.
	if err := binary.Read(r, binary.LittleEndian, &rfs.Directory.SubObjectsSize); err != nil {
		return rfs, fmt.Errorf("unabel to decode rootfs directory total subobject size: %s", err)
	}

	return rfs, nil
}
