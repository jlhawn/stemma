// +build darwin

package sysutil

// GetXattrs gets all of the xattrs for the file at the given path.
func GetXattrs(path string) (Xattrs, error) {
	return nil, nil // Not currently supported on Mac OS X.
}

// GetXattr gets the value of the specified attr for the file at the given
// path.
func GetXattr(path, attr string) ([]byte, error) {
	return nil, nil // Not currently supported on Mac OS X.
}
