package xattr

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

// GetXattrs gets all of the xattrs for the file at the given path.
func GetXattrs(path string) (map[string]string, error) {
	sz, err := unix.Listxattr(path, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to get xattr list size: %s", err)
	}

	buf := make([]byte, sz)
	if sz, err = unix.Listxattr(path, buf); err != nil {
		return nil, fmt.Errorf("unable to get xattr list: %s", err)
	}

	if sz == 0 {
		return nil, nil
	}

	buf = buf[:sz-1] // Trim off trailing \x00.

	attrNames := strings.Split(string(buf), "\x00")
	xattrs := make(map[string]string, len(attrNames))

	for _, attrName := range attrNames {
		attrVal, err := GetXattr(path, attrName)
		if err != nil {
			return nil, err
		}

		xattrs[attrName] = attrVal
	}

	return xattrs, nil
}

// GetXattr gets the value of the specified attr for the file at the given
// path.
func GetXattr(path, attr string) (string, error) {
	sz, err := unix.Getxattr(path, attr, nil)
	if err != nil {
		return "", fmt.Errorf("unable to get xattr value size: %s", err)
	}

	buf := make([]byte, sz)
	if sz, err = unix.Getxattr(path, attr, buf); err != nil {
		return "", fmt.Errorf("unable to get xattr value: %s", err)
	}

	return string(buf[:sz]), nil
}
