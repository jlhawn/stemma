package main

import (
	"crypto"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "crypto/sha256"

	"github.com/jlhawn/blobstore"
	"github.com/jlhawn/stemma/xattr"
	"golang.org/x/sys/unix"
)

type header struct {
	Size          int64             `json:"size"`
	Mtime         unix.Timespec     `json:"mtime"`
	Nlink         uint64            `json:"nlink"`
	UID           uint32            `json:"uid"`
	GID           uint32            `json:"gid"`
	Mode          os.FileMode       `json:"mode"`
	Rdev          uint64            `json:"rdev"`
	Xattrs        map[string]string `json:"xattrs"`
	ContentDigest string            `json:"contentDigest"`
	NumObjects    uint64            `json:"numObjects"`
}

type treeNode struct {
	name       string
	mode       os.FileMode
	dirEntries map[string]*treeNode
}

func (t *treeNode) insert(targpath string, o *treeNode) error {
	if !t.mode.IsDir() {
		return errors.New("not a directory node")
	}

	relPath, err := filepath.Rel(t.name, targpath)
	if err != nil {
		return err
	}

	parts := strings.SplitN(relPath, string(filepath.Separator), 2)
	name := parts[0]
	child, exists := t.dirEntries[name]

	if len(parts) == 1 {
		// There shouldn't be an existing entry.
		if exists {
			return errors.New("node already exists")
		}

		t.dirEntries[name] = o
		return nil
	}

	// There should be an existing directory node.
	if !exists {
		return errors.New("directory node does not exist")
	}

	return child.insert(relPath, o)
}

func (t *treeNode) write(w io.Writer, indent string) error {
	_, err := fmt.Fprintf(w, "%s%s", indent, t.name)

	if err == nil && t.mode.IsDir() && !strings.HasSuffix(t.name, string(filepath.Separator)) {
		_, err = fmt.Fprintf(w, "%c", filepath.Separator)
	}

	if err == nil {
		_, err = fmt.Fprintln(w)
	}

	for _, child := range t.dirEntries {
		if err != nil {
			break
		}

		err = child.write(w, indent+"  ")
	}

	return err
}

func (t *treeNode) store(s blobstore.Store, path string) (digest string, numObjects uint64, err error) {
	contentDigest, numObjects, err := t.storeContent(s, path)
	if err != nil {
		return "", 0, err
	}

	defer func() {
		if err != nil {
			s.Deref(contentDigest)
		} else if contentDigest != "" {
			err = s.Link(contentDigest, digest)
		}
	}()

	return storeHeader(s, contentDigest, path, numObjects)
}

func (t *treeNode) storeContent(s blobstore.Store, path string) (digest string, numObjects uint64, err error) {
	switch {
	case t.mode.IsDir():
		return t.storeDirContent(s, path)
	case t.mode.IsRegular():
		return storeFileContent(s, path)
	case t.mode&os.ModeSymlink != 0:
		// Node is a symbolic link.
		return storeSymLink(s, path)
	default:
		// Don't store content for special files.
		return "", 0, nil
	}
}

func (t *treeNode) storeDirContent(s blobstore.Store, dirname string) (digest string, numObjects uint64, err error) {
	entries := make(map[string]string, len(t.dirEntries))

	for _, child := range t.dirEntries {
		var (
			childDigest     string
			numChildObjects uint64
		)
		if childDigest, numChildObjects, err = child.store(s, filepath.Join(dirname, child.name)); err != nil {
			return "", 0, err
		}

		numObjects += numChildObjects

		defer func(childDigest string) {
			if err != nil {
				s.Deref(childDigest)
			} else {
				err = s.Link(childDigest, digest)
			}
		}(childDigest)

		entries[child.name] = childDigest
	}

	bw, err := s.NewWriter(crypto.SHA256)
	if err != nil {
		return "", 0, err
	}

	defer func() {
		if err != nil {
			bw.Cancel()
		}
	}()

	if err = json.NewEncoder(bw).Encode(entries); err != nil {
		return "", 0, err
	}

	desc, err := bw.Commit()
	if err != nil {
		return "", 0, err
	}

	numObjects++ // Counting this object.

	return desc.Digest(), numObjects, nil
}

func storeFileContent(s blobstore.Store, filename string) (digest string, numObjects uint64, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()

	bw, err := s.NewWriter(crypto.SHA256)
	if err != nil {
		return "", 0, err
	}

	defer func() {
		if err != nil {
			bw.Cancel()
		}
	}()

	if _, err = io.Copy(bw, file); err != nil {
		return "", 0, err
	}

	desc, err := bw.Commit()
	if err != nil {
		return "", 0, err
	}

	return desc.Digest(), 1, nil
}

func storeSymLink(s blobstore.Store, linkname string) (digest string, numObjects uint64, err error) {
	target, err := os.Readlink(linkname)
	if err != nil {
		return "", 0, err
	}

	bw, err := s.NewWriter(crypto.SHA256)
	if err != nil {
		return "", 0, err
	}

	defer func() {
		if err != nil {
			bw.Cancel()
		}
	}()

	if _, err = io.Copy(bw, strings.NewReader(target)); err != nil {
		return "", 0, err
	}

	desc, err := bw.Commit()
	if err != nil {
		return "", 0, err
	}

	return desc.Digest(), 1, nil
}

func storeHeader(s blobstore.Store, contentDigest, fullPath string, numContentObjects uint64) (digest string, numObjects uint64, err error) {
	var stat unix.Stat_t
	if err = unix.Lstat(fullPath, &stat); err != nil {
		return "", 0, err
	}

	hdr := header{
		Size:          stat.Size,
		Mtime:         stat.Mtim,
		Nlink:         stat.Nlink,
		UID:           stat.Uid,
		GID:           stat.Gid,
		Mode:          os.FileMode(stat.Mode),
		Rdev:          stat.Rdev,
		ContentDigest: contentDigest,
		NumObjects:    numContentObjects,
	}

	xattrs, err := xattr.GetXattrs(fullPath)
	if err != nil {
		return "", 0, err
	}

	hdr.Xattrs = xattrs

	bw, err := s.NewWriter(crypto.SHA256)
	if err != nil {
		return "", 0, err
	}

	defer func() {
		if err != nil {
			bw.Cancel()
		}
	}()

	if err = json.NewEncoder(bw).Encode(hdr); err != nil {
		return "", 0, err
	}

	desc, err := bw.Commit()
	if err != nil {
		return "", 0, err
	}

	numObjects = numContentObjects + 1 // Count this header object.

	return desc.Digest(), numObjects, nil
}

func main() {
	var root *treeNode

	rootPath := "/home/jlhawn"

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		node := &treeNode{
			name: info.Name(),
			mode: info.Mode(),
		}

		if node.mode.IsDir() {
			node.dirEntries = map[string]*treeNode{}
		}

		if root == nil {
			root = node
			return nil
		}

		relPath, err := filepath.Rel(rootPath, path)
		if err != nil {
			return err
		}

		return root.insert(filepath.Join(root.name, relPath), node)
	})

	if err != nil {
		log.Fatal(err)
	}

	s, err := blobstore.NewLocalStore("/blobstore")
	if err != nil {
		log.Fatal(err)
	}

	digest, numObjects, err := root.store(s, rootPath)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("root node digest: %s\n", digest)
	fmt.Printf("total objects: %d\n", numObjects)

	digests, err := s.List()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%d blobs total\n", len(digests))

	blob, err := s.Get(digest)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("root node refCount: %d\n", blob.RefCount())

	fmt.Scanln()

	if err := s.Deref(digest); err != nil {
		log.Fatal(err)
	}

	digests, err = s.List()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%d blobs remaining\n", len(digests))
}
