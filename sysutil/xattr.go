package sysutil

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// Xattr is an extended attribute key-value pair.
type Xattr struct {
	Key string
	Val []byte
}

// Xattrs is a list of extended attributes. Implements sort.Interface.
type Xattrs []Xattr

// NewXattrs creates an ordered list of extended attributes from the given map.
func NewXattrs(m map[string][]byte) Xattrs {
	xattrs := make(Xattrs, 0, len(m))
	for key, val := range m {
		xattrs = append(xattrs, Xattr{
			Key: key,
			Val: val,
		})
	}

	sort.Sort(xattrs)

	return xattrs
}

// Map creates a map from this list of extended attributes.
func (x Xattrs) Map() map[string][]byte {
	m := make(map[string][]byte, len(x))
	for _, attr := range x {
		m[attr.Key] = attr.Val
	}

	return m
}

func (x Xattrs) Len() int {
	return len(x)
}

func (x Xattrs) Less(i, j int) bool {
	return x[i].Key < x[j].Key
}

func (x Xattrs) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

// Marshal marshals the binary encoding (little-endian) of this list of
// extended attributes into the given writer.
func (x Xattrs) Marshal(w io.Writer) error {
	// Use a 16-bit value to store the length. I can't imagine a situation
	// where a file has more than 64k extended attributes so this should
	// be enough.
	numXattrs := uint16(len(x))
	if err := binary.Write(w, binary.LittleEndian, numXattrs); err != nil {
		return fmt.Errorf("unable to encode length of Xattrs list: %s", err)
	}

	for _, xattr := range x {
		keyBuf := []byte(xattr.Key)
		keyLen := uint16(len(keyBuf))

		// Write the key length. Most OS/Filesystems don't allow keys
		// longer than 256 bytes, but we will use a 16-bit number to
		// make (un)marshaling easy.
		if err := binary.Write(w, binary.LittleEndian, keyLen); err != nil {
			return fmt.Errorf("unable to encode xattr key length: %s", err)
		}

		// Write the key bytes.
		if _, err := w.Write(keyBuf); err != nil {
			return fmt.Errorf("unable to encode xattr key bytes: %s", err)
		}

		valLen := uint16(len(xattr.Val))

		// Write the value length. Most OS/Filesystems don't allow
		// extended attribute values longer than 64 kilobytes anyway.
		if err := binary.Write(w, binary.LittleEndian, valLen); err != nil {
			return fmt.Errorf("unable to encode xattr value length: %s", err)
		}

		// Write the value bytes.
		if _, err := w.Write(xattr.Val); err != nil {
			return fmt.Errorf("unable to encode xattr value bytes: %s", err)
		}
	}

	return nil
}

// UnmarshalXattrs unmarshals the binary encoding (little-endian) of a list of
// extended attributes from the given reader.
func UnmarshalXattrs(r io.Reader) (Xattrs, error) {
	var numXattrs uint16
	if err := binary.Read(r, binary.LittleEndian, &numXattrs); err != nil {
		return nil, fmt.Errorf("unable to decode length of Xattrs list: %s", err)
	}

	xattrs := make(Xattrs, numXattrs)
	for i := range xattrs {
		var keyLen uint16
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("unable to decode xattr key length: %s", err)
		}

		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			return nil, fmt.Errorf("unable to decode xattr key bytes: %s", err)
		}

		xattrs[i].Key = string(keyBuf)

		var valLen uint16
		if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
			return nil, fmt.Errorf("unable to decode xattr value length: %s", err)
		}

		xattrs[i].Val = make([]byte, valLen)
		if _, err := io.ReadFull(r, xattrs[i].Val); err != nil {
			return nil, fmt.Errorf("unable to decode xattre value bytes: %s", err)
		}
	}

	return xattrs, nil
}
