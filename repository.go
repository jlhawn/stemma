package stemma

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/jlhawn/stemma/sysutil"
)

// Repository represents a content-addressable repository of filesystem objects
// and application container metadata.
type Repository struct {
	root string
	// Advisory lock for the repository. Acquire an exclusive lock
	// if your opperation may modify refs or delete objects. Acquire a
	// shared lock if your opperation will only be reading refs or reading
	// or writing objects. Note: a shared lock for writing objects is okay
	// due to the content-addressibility of the object store.
	*sysutil.Lock

	tags   TagStore
	mounts MountSet
}

var _ ObjectStore = &Repository{}

// NewRepository returns a repository storing objects at the given root
// directory.
func NewRepository(root string) (*Repository, error) {
	rootDir, err := os.Open(root)
	if err != nil {
		return nil, fmt.Errorf("unable to open directory %q: %s", root, err)
	}

	if fi, err := rootDir.Stat(); err != nil {
		return nil, fmt.Errorf("unable to stat directory %q: %s", root, err)
	} else if !fi.IsDir() {
		return nil, fmt.Errorf("unable to use directory %q: not a directory", root)
	}

	objectsDirPath := filepath.Join(root, "objects")
	if err := os.MkdirAll(objectsDirPath, os.FileMode(0755)); err != nil {
		return nil, fmt.Errorf("unable to make objects directory: %s", err)
	}

	tagsDirPath := filepath.Join(root, "refs", "tags")
	if err := os.MkdirAll(tagsDirPath, os.FileMode(0755)); err != nil {
		return nil, fmt.Errorf("unable to make tags directory: %s", err)
	}

	tagStore, err := NewTagStore(tagsDirPath)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize tag store: %s", err)
	}

	return &Repository{
		root: root,
		Lock: sysutil.NewLock(rootDir),
		tags: tagStore,
	}, nil
}

// TagStore returns the Tag Store for this repository.
func (r *Repository) TagStore() TagStore {
	return r.tags
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
		return nil, fmt.Errorf("unable to open file %q: %s", path, err)
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
