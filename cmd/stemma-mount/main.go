package main

import (
	"errors"
	"flag"
	"fmt"
	"hash/crc64"
	"log"
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/jlhawn/stemma"
	"golang.org/x/net/context"
)

var errNotImplemented = errors.New("not implemented")

func main() {
	fuse.Debug = func(msg interface{}) {
		log.Print(msg)
	}

	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("Usage: stemma-mount DIGEST|TAG MOUNTPOINT")
		os.Exit(1)
	}

	repo, err := stemma.NewRepository(".")
	if err != nil {
		log.Fatalf("unable to initialize repository: %s", err)
	}

	appDigest, err := repo.ResolveRef(flag.Arg(0))
	if err != nil {
		log.Fatalf("unable to resolve reference: %s", err)
	}

	conn, err := fuse.Mount(
		flag.Arg(1),
		fuse.AllowOther(), fuse.DefaultPermissions(),
		fuse.FSName("stemma"), fuse.LocalVolume(),
		fuse.Subtype("stemma"), fuse.VolumeName("stemma"),
		fuse.AllowDev(), fuse.AllowSUID(),
	)
	if err != nil {
		log.Fatalf("unable to mount filesytem: %s", err)
	}
	defer conn.Close()
	defer fuse.Unmount(flag.Arg(1))

	filesystem, err := newFS(repo, appDigest)
	if err != nil {
		log.Fatalf("unable to initialize filesystem root: %s", err)
	}

	if err := fs.Serve(conn, filesystem); err != nil {
		log.Fatalf("unable to server filesystem: %s", err)
	}

	// Check if the mount process has an error to report.
	<-conn.Ready
	if err := conn.MountError; err != nil {
		log.Fatal(err)
	}
}

// FS implements a read-only FUSE filesystem backed by a content-addressable
// object store.
type FS struct {
	root fs.Node
}

func newFS(objects stemma.ObjectStore, appDigest stemma.Digest) (*FS, error) {
	app, err := objects.GetApplication(appDigest)
	if err != nil {
		return nil, fmt.Errorf("unable to get application from object store: %s", err)
	}

	entry := stemma.DirectoryEntry{
		Name:           "/",
		Type:           stemma.DirentTypeDirectory,
		HeaderDigest:   app.Rootfs.Header.Digest,
		HeaderSize:     app.Rootfs.Header.Size,
		ObjectDigest:   app.Rootfs.Directory.Digest,
		ObjectSize:     app.Rootfs.Directory.Size,
		NumSubObjects:  app.Rootfs.Directory.NumSubObjects,
		SubObjectsSize: app.Rootfs.Directory.SubObjectsSize,
	}

	rootNode, err := subNode(objects, entry, time.Now())
	if err != nil {
		return nil, fmt.Errorf("unable to make root node: %s", err)
	}

	return &FS{
		root: rootNode,
	}, nil
}

// Root is called to obtain the Node for the file system root.
func (fs *FS) Root() (fs.Node, error) {
	return fs.root, nil
}

// attr contains common file attributes to meet the FUSE Attr() request.
// TODO: get object store and header digest references to serve Xattrs.
type attr struct {
	inode uint64      // inode number
	size  uint64      // size in bytes
	time  time.Time   // time of last access, modification, change, creation
	mode  os.FileMode // file mode
	uid   uint32      // owner uid
	gid   uint32      // group gid
	rdev  uint32      // device numbers
}

// Attr fills attr with the standard metadata for the node.
func (a *attr) Attr(ctx context.Context, attr *fuse.Attr) error {
	*attr = fuse.Attr{
		Inode:     a.inode,
		Size:      a.size,
		Atime:     a.time,
		Mtime:     a.time,
		Ctime:     a.time,
		Crtime:    a.time,
		Mode:      a.mode,
		Nlink:     1,
		Uid:       a.uid,
		Gid:       a.gid,
		Rdev:      a.rdev,
		BlockSize: 4096,
	}

	return nil
}

var ecmaTable = crc64.MakeTable(crc64.ECMA)

// inode computes a random/unique inode number for the given directory
// entry. The indode number should identify unique (header, object) pairs
// so we can't just use the object digest. We should also consider the
// symlink target. DO NOT consider the name of the entry.
func inode(de stemma.DirectoryEntry) uint64 {
	hash := crc64.New(ecmaTable)

	hash.Write([]byte(de.HeaderDigest))
	hash.Write([]byte(de.ObjectDigest))
	hash.Write([]byte(de.LinkTarget))

	return hash.Sum64()
}

// fuseDirentTypes stores a mapping of stemma dirent types to fuse dirent
// types. Note that if the dirent type is unknown, the default zero value
// corresponds to the unknown fuse dirent type.
var fuseDirentTypes = map[stemma.DirentType]fuse.DirentType{
	stemma.DirentTypeBlockDevice: fuse.DT_Block,
	stemma.DirentTypeCharDevice:  fuse.DT_Char,
	stemma.DirentTypeDirectory:   fuse.DT_Dir,
	stemma.DirentTypeFifo:        fuse.DT_FIFO,
	stemma.DirentTypeLink:        fuse.DT_Link,
	stemma.DirentTypeRegular:     fuse.DT_File,
	stemma.DirentTypeSocket:      fuse.DT_Socket,
}

// Dir implements both Node and Handle for a directory.
type Dir struct {
	*attr
	objects stemma.ObjectStore
	digest  stemma.Digest
}

// ReadDirAll returns a list of entries from this directory.
func (d *Dir) ReadDirAll(ctx context.Context) (fuseEntries []fuse.Dirent, err error) {
	directory, err := d.objects.GetDirectory(d.digest)
	if err != nil {
		return nil, fmt.Errorf("unable to get directory from object store: %s", err)
	}

	fuseEntries = make([]fuse.Dirent, len(directory))
	for i, entry := range directory {
		fuseEntries[i] = fuse.Dirent{
			Inode: inode(entry),
			Type:  fuseDirentTypes[entry.Type],
			Name:  entry.Name,
		}
	}

	return fuseEntries, nil
}

// Lookup looks up a specific entry in the receiver,
// which must be a directory.  Lookup should return a Node
// corresponding to the entry.  If the name does not exist in
// the directory, Lookup should return ENOENT.
//
// Lookup need not to handle the names "." and "..".
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	directory, err := d.objects.GetDirectory(d.digest)
	if err != nil {
		return nil, fmt.Errorf("unable to get directory from object store: %s", err)
	}

	for _, entry := range directory {
		if entry.Name != name {
			continue
		}

		return subNode(d.objects, entry, d.attr.time)
	}

	return nil, fuse.ENOENT
}

func subNode(objects stemma.ObjectStore, entry stemma.DirectoryEntry, nodeTime time.Time) (fs.Node, error) {
	header, err := objects.GetHeader(entry.HeaderDigest)
	if err != nil {
		return nil, fmt.Errorf("unable to get header from object store: %s", err)
	}

	attrBase := &attr{
		inode: inode(entry),
		size:  entry.ObjectSize,
		time:  nodeTime,
		mode:  header.Mode,
		uid:   header.UID,
		gid:   header.GID,
		rdev:  header.Rdev,
	}

	switch entry.Type {
	case stemma.DirentTypeDirectory:
		return &Dir{
			attr:    attrBase,
			objects: objects,
			digest:  entry.ObjectDigest,
		}, nil
	case stemma.DirentTypeRegular:
		return &File{
			attr:    attrBase,
			objects: objects,
			digest:  entry.ObjectDigest,
		}, nil
	case stemma.DirentTypeLink:
		return &Link{
			attr:   attrBase,
			target: entry.LinkTarget,
		}, nil
	default:
		return attrBase, nil
	}
}

// File represents a file node.
type File struct {
	*attr
	objects stemma.ObjectStore
	digest  stemma.Digest
	rsc     stemma.ReadSeekCloser
}

// Open opens the receiver. After a successful open, a client
// process has a file descriptor referring to this Handle.
//
// Open can also be also called on non-files. For example,
// directories are Opened for ReadDir or fchdir(2).
//
// If this method is not implemented, the open will always
// succeed, and the Node itself will be used as the Handle.
//
// XXX note about access.  XXX OpenFlags.
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	rsc, err := f.objects.GetFile(f.digest)
	if err != nil {
		return nil, fmt.Errorf("unable to get file from object store: %s", err)
	}

	return &FileHandle{rsc: rsc}, nil
}

// FileHandle represents an open file handle.
type FileHandle struct {
	rsc stemma.ReadSeekCloser
}

// Read requests to read data from the handle.
//
// There is a page cache in the kernel that normally submits only
// page-aligned reads spanning one or more pages. However, you
// should not rely on this. To see individual requests as
// submitted by the file system clients, set OpenDirectIO.
//
// Note that reads beyond the size of the file as reported by Attr
// are not even attempted (except in OpenDirectIO mode).
func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	if _, err := fh.rsc.Seek(req.Offset, os.SEEK_SET); err != nil {
		return fmt.Errorf("unable to seek: %s", err)
	}

	resp.Data = make([]byte, req.Size)
	n, err := fh.rsc.Read(resp.Data)
	resp.Data = resp.Data[:n]

	return err
}

// Release asks to release (close) an open file handle.
func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return fh.rsc.Close()
}

// Link implements both Node and Handle for a symbolic link.
type Link struct {
	*attr
	target string
}

// Readlink reads a symbolic link.
func (l *Link) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return l.target, nil
}
