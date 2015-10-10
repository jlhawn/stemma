package stemma

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Repository represents a content-addressable repository of filesystem objects
// and application container metadata.
type Repository struct {
	root string

	tags   TagStore
	mounts MountSet
}

// NewRepository returns a repository storing objects at the given root
// directory.
func NewRepository(root string) *Repository {
	return &Repository{
		root: root,
	}
}

func (r *Repository) getObjectPath(digest Digest) string {
	digestHex := digest.Hex()
	return filepath.Join(r.root, "objects", digestHex[:2], digestHex[2:4], digestHex[4:6], digestHex[6:])
}

func (r *Repository) getObjectFile(digest Digest) (io.ReadCloser, error) {
	objectPath := r.getObjectPath(digest)

	return os.Open(objectPath)
}

func (r *Repository) tempFile() (*os.File, error) {
	tempDir := filepath.Join(r.root, "temp")
	if err := os.MkdirAll(tempDir, os.FileMode(0755)); err != nil {
		return nil, fmt.Errorf("unable to make temp directory: %s", err)
	}

	return ioutil.TempFile(tempDir, "")
}

/*
Repository Layout:

	objects/
	temp/
	refs/
		mounts/
		tags/

*/

// StoreFile stores the file at the given path in this repository.
func (r *Repository) StoreFile(path string) (Descriptor, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("unable to open file %q: %s", err)
	}
	defer file.Close()

	fileWriter, err := r.NewFileWriter()
	if err != nil {
		return nil, fmt.Errorf("unable to get new file writer: %s", err)
	}

	if _, err := io.Copy(fileWriter, file); err != nil {
		fileWriter.Cancel()
		return nil, fmt.Errorf("unable to store file: %s", err)
	}

	return fileWriter.Commit()
}

// StoreDirectory recursively stores the directory at the given path in this
// repository.
func (r *Repository) StoreDirectory(path string) (Descriptor, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("unable to open directory %q: %s", path, err)
	}
	defer dir.Close()

	entryNames, err := dir.Readdirnames(0)
	if err != nil {
		return nil, fmt.Errorf("unable to read directory %q entries: %s", path, err)
	}

	dirWriter, err := r.NewDirectoryWriter(uint(len(entryNames)))
	if err != nil {
		return nil, fmt.Errorf("unable to get new directory writer: %s", err)
	}

	for _, entryName := range entryNames {
		entryPath := filepath.Join(path, entryName)

		header, err := NewHeader(entryPath)
		if err != nil {
			return nil, fmt.Errorf("unable to get object header %q: %s", entryPath, err)
		}

		headerDescriptor, err := r.PutHeader(header)
		if err != nil {
			return nil, fmt.Errorf("unable to store header for directory entry %q: %s", entryName, err)
		}

		entry := DirectoryEntry{
			Name:         entryName,
			Type:         header.DirentType(),
			HeaderDigest: headerDescriptor.Digest(),
			HeaderSize:   headerDescriptor.Size(),
		}

		var objectDescriptor Descriptor
		switch entry.Type {
		case DirentTypeDirectory:
			objectDescriptor, err = r.StoreDirectory(entryPath)
			if err != nil {
				return nil, fmt.Errorf("unable to store subdirectory %q: %s", entryPath, err)
			}
		case DirentTypeRegular:
			objectDescriptor, err = r.StoreFile(entryPath)
			if err != nil {
				return nil, fmt.Errorf("unable to store file %q: %s", entryPath, err)
			}
		case DirentTypeLink:
			if entry.LinkTarget, err = os.Readlink(entryPath); err != nil {
				return nil, fmt.Errorf("unable to read link target %q: %s", entryPath, err)
			}
		}

		if objectDescriptor != nil {
			entry.ObjectDigest = objectDescriptor.Digest()
			entry.ObjectSize = objectDescriptor.Size()

			entry.NumSubObjects = objectDescriptor.NumSubObjects()
			entry.SubObjectsSize = objectDescriptor.SubObjectsSize()
		}

		dirWriter.Add(entry)
	}

	dirDescriptor, err := dirWriter.Commit()
	if err != nil {
		return nil, fmt.Errorf("unable to commit directory %q: %s", path, err)
	}

	return dirDescriptor, nil
}
