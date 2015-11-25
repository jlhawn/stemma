package stemma

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/jlhawn/stemma/sysutil"
)

// Header provides all filesystem header fields. Note that file size is not
// stored in this header as it is stored in the directory entry object instead.
// Datetimes for creation, last-modified, last-accessed, and last-change are
// not stored at all. This allows for many objects with the same mode and
// ownership to use the same header object whether they would not be able to if
// they had differing sizes or timestamps.
type Header struct {
	Mode   os.FileMode       // File mode
	Rdev   uint32            // Device numbers
	UID    uint32            // Owner uid
	GID    uint32            // Group gid
	Xattrs map[string][]byte // Extended attributes
}

// NewHeader returns a new Header for the object at the given path.
func NewHeader(path string) (Header, error) {
	var stat syscall.Stat_t
	if err := syscall.Lstat(path, &stat); err != nil {
		return Header{}, fmt.Errorf("unable to stat path %q: %s", path, err)
	}

	mode := os.FileMode(stat.Mode & 0777)
	switch stat.Mode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		mode |= os.ModeDevice
	case syscall.S_IFCHR:
		mode |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFDIR:
		mode |= os.ModeDir
	case syscall.S_IFIFO:
		mode |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		mode |= os.ModeSymlink
	case syscall.S_IFREG:
		// nothing to do
	case syscall.S_IFSOCK:
		mode |= os.ModeSocket
	}
	if mode&syscall.S_ISGID != 0 {
		mode |= os.ModeSetgid
	}
	if mode&syscall.S_ISUID != 0 {
		mode |= os.ModeSetuid
	}
	if mode&syscall.S_ISVTX != 0 {
		mode |= os.ModeSticky
	}

	xattrs, err := sysutil.GetXattrs(path)
	if err != nil {
		return Header{}, fmt.Errorf("unable to get xattrs for path %q: %s", path, err)
	}

	return Header{
		Mode:   mode,
		Rdev:   uint32(stat.Rdev),
		UID:    stat.Uid,
		GID:    stat.Gid,
		Xattrs: xattrs.Map(),
	}, nil
}

// DirentType returns the directory entry type for this header.
func (h Header) DirentType() DirentType {
	switch h.Mode & os.ModeType {
	case 0:
		return DirentTypeRegular
	case os.ModeDevice:
		if h.Mode&os.ModeCharDevice != 0 {
			return DirentTypeCharDevice
		}
		return DirentTypeBlockDevice
	case os.ModeDir:
		return DirentTypeDirectory
	case os.ModeNamedPipe:
		return DirentTypeFifo
	case os.ModeSymlink:
		return DirentTypeLink
	case os.ModeSocket:
		return DirentTypeSocket
	default:
		return DirentTypeUnknown
	}
}

// Marshal marshals the binary encoding (little-endian) of this header into
// the given writer.
func (h Header) Marshal(w io.Writer) error {
	vals := []struct {
		field string
		value uint32
	}{
		{"mode", uint32(h.Mode)},
		{"rdev", h.Rdev},
		{"uid", h.UID},
		{"gid", h.GID},
	}

	for _, val := range vals {
		if err := binary.Write(w, binary.LittleEndian, val.value); err != nil {
			return fmt.Errorf("unable to encode header value %q: %s", val.field, err)
		}
	}

	if err := sysutil.NewXattrs(h.Xattrs).Marshal(w); err != nil {
		return fmt.Errorf("unable to encode header xattrs: %s", err)
	}

	return nil
}

// UnmarshalHeader unmarshals the binary encoding (little-endian) of a header
// from the given reader.
func UnmarshalHeader(r io.Reader) (header Header, err error) {
	vals := []struct {
		field string
		value *uint32
	}{
		{"mode", (*uint32)(&header.Mode)},
		{"rdev", &header.Rdev},
		{"uid", &header.UID},
		{"gid", &header.GID},
	}

	for _, val := range vals {
		if err := binary.Read(r, binary.LittleEndian, val.value); err != nil {
			return header, fmt.Errorf("unable to decode header value %q: %s", val.field, err)
		}
	}

	xattrs, err := sysutil.UnmarshalXattrs(r)
	if err != nil {
		return header, fmt.Errorf("unable to decode header xattrs: %s", err)
	}

	header.Xattrs = xattrs.Map()

	return header, nil
}

// GetHeader gets the header object with the given digest from this repository.
func (r *Repository) GetHeader(digest Digest) (header Header, err error) {
	object, err := r.getObjectFile(digest)
	if err != nil {
		return header, fmt.Errorf("unable to get header object: %s", err)
	}

	defer object.Close()

	header, err = UnmarshalHeader(object)
	if err != nil {
		return header, fmt.Errorf("unable to decode header object: %s", err)
	}

	return header, nil
}

// PutHeader puts the given header into this repository.
func (r *Repository) PutHeader(header Header) (Descriptor, error) {
	objectWriter, err := r.newObjectWriter(ObjectTypeHeader)
	if err != nil {
		return nil, fmt.Errorf("unable to get new object writer: %s", err)
	}

	if err := header.Marshal(objectWriter); err != nil {
		objectWriter.Cancel()
		return nil, fmt.Errorf("unable to encode header object: %s", err)
	}

	return objectWriter.Commit()
}
