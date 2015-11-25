package main

import (
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
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

type nodeRef struct {
	node  fs.Node
	count uint
}

// FS implements a read-only FUSE filesystem backed by a content-addressable
// object store.
type FS struct {
	objects stemma.ObjectStore
	root    fs.Node
	time    time.Time

	// A cache of valid nodes mapped by inode number. Nodes are reference
	// counted. An inode number should always map to the same node due to
	// content-addressability.
	nodeRefs map[uint64]nodeRef
	sync.Mutex
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

	fs := &FS{
		objects:  objects,
		nodeRefs: make(map[uint64]nodeRef, 1024),
		time:     time.Now(),
	}

	var rootInode uint64
	if fs.root, err = fs.makeNode(entry, rootInode); err != nil {
		return nil, fmt.Errorf("unable to make root node: %s", err)
	}

	// Hack to make the root dir have the same inode and parent inode.
	fs.root.(*Dir).inode = rootInode

	return fs, nil
}

// Root is called to obtain the Node for the file system root.
func (fs *FS) Root() (fs.Node, error) {
	return fs.root, nil
}

func (fs *FS) refNode(inode uint64) fs.Node {
	fs.Lock()
	defer fs.Unlock()

	if ref, ok := fs.nodeRefs[inode]; ok {
		fs.nodeRefs[inode] = nodeRef{
			node:  ref.node,
			count: ref.count + 1,
		}
		return ref.node
	}

	return nil
}

func (fs *FS) derefNode(inode uint64) {
	fs.Lock()
	defer fs.Unlock()

	ref, ok := fs.nodeRefs[inode]
	if !ok {
		return // Nothing to do.
	}

	if ref.count <= 1 {
		delete(fs.nodeRefs, inode)
	} else {
		fs.nodeRefs[inode] = nodeRef{
			node:  ref.node,
			count: ref.count - 1,
		}
	}
}

func (fs *FS) makeNode(entry stemma.DirectoryEntry, parent uint64) (node fs.Node, err error) {
	inodeNum := inode(entry, parent)

	// Reuse a cached node if possible.
	if node = fs.refNode(inodeNum); node != nil {
		return node, nil
	}

	header, err := fs.objects.GetHeader(entry.HeaderDigest)
	if err != nil {
		return nil, fmt.Errorf("unable to get header from object store: %s", err)
	}

	attrBase := &attr{
		fs:    fs,
		inode: inodeNum,
		size:  entry.ObjectSize,
		time:  fs.time,
		mode:  header.Mode,
		uid:   header.UID,
		gid:   header.GID,
		rdev:  header.Rdev,
	}

	switch entry.Type {
	case stemma.DirentTypeDirectory:
		node = &Dir{
			attr:   attrBase,
			parent: parent,
			digest: entry.ObjectDigest,
		}
	case stemma.DirentTypeRegular:
		node = &File{
			attr:   attrBase,
			digest: entry.ObjectDigest,
		}
	case stemma.DirentTypeLink:
		node = &Link{
			attr:   attrBase,
			target: entry.LinkTarget,
		}
	default:
		node = attrBase
	}

	// Cache this node or get one that already exists (in case of race).
	fs.Lock()
	defer fs.Unlock()

	if ref, ok := fs.nodeRefs[inodeNum]; ok {
		fs.nodeRefs[inodeNum] = nodeRef{
			node:  ref.node,
			count: ref.count + 1,
		}
		node = ref.node
	} else {
		fs.nodeRefs[inodeNum] = nodeRef{
			node:  node,
			count: 1,
		}
	}

	return node, nil
}

// attr contains common file attributes to meet the FUSE Attr() request.
// TODO: get object store and header digest references to serve Xattrs.
type attr struct {
	fs *FS

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
		// Cache for only a minute. The filesystem is read-only so we
		// could probably just cache forever. This needs to be looked
		// into further.
		Valid:     time.Minute,
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

// Forget about this node. This node will not receive further
// method calls.
//
// Forget is not necessarily seen on unmount, as all nodes are
// implicitly forgotten as part part of the unmount.
func (a *attr) Forget() {
	a.fs.derefNode(a.inode)
}

// inode computes a random/unique inode number for the given directory
// entry. The indode number should identify unique (header, object) pairs
// so we can't just use the object digest. We should also consider the
// symlink target. DO NOT consider the name of the entry. If the entry is a
// directory, the parent inode number will be hashed into the value as well.
func inode(de stemma.DirectoryEntry, parent uint64) uint64 {
	hash := sha512.New()

	if de.Type == stemma.DirentTypeDirectory {
		// Only hash the parent inode if this is a directory. This
		// helps ensure that no 2 directories have the same inode as
		// hard links to directories are not allowed (what would ".."
		// mean?).
		binary.Write(hash, binary.LittleEndian, parent)
	}

	hash.Write([]byte(de.HeaderDigest))
	hash.Write([]byte(de.ObjectDigest))
	hash.Write([]byte(de.LinkTarget))

	return binary.LittleEndian.Uint64(hash.Sum(nil))
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

// Dir represents a directory node.
type Dir struct {
	*attr
	parent uint64
	digest stemma.Digest

	entries    stemma.Directory
	entryIndex map[string]int
}

func (d *Dir) load() error {
	entries, err := d.fs.objects.GetDirectory(d.digest)
	if err != nil {
		return fmt.Errorf("unable to get directory object store: %s", err)
	}

	d.entries = entries
	d.entryIndex = make(map[string]int, len(entries))
	for i, de := range entries {
		d.entryIndex[de.Name] = i
	}

	return nil
}

// ReadDirAll returns a list of entries from this directory.
func (d *Dir) ReadDirAll(ctx context.Context) (fuseEntries []fuse.Dirent, err error) {
	// Ensure that entries have been loaded.
	if d.entries == nil {
		if err := d.load(); err != nil {
			return nil, err
		}
	}

	currentDir := fuse.Dirent{
		Inode: d.inode,
		Type:  fuse.DT_Dir,
		Name:  ".",
	}
	parentDir := fuse.Dirent{
		Inode: d.parent,
		Type:  fuse.DT_Dir,
		Name:  "..",
	}

	// Allocate space for every directory entry, including "." and "..".
	fuseEntries = make([]fuse.Dirent, 0, len(d.entries)+2)
	fuseEntries = append(fuseEntries, currentDir, parentDir)

	for _, entry := range d.entries {
		fuseEntries = append(fuseEntries, fuse.Dirent{
			Inode: inode(entry, d.inode),
			Type:  fuseDirentTypes[entry.Type],
			Name:  entry.Name,
		})
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
	// Ensure that entries have been loaded.
	if d.entries == nil {
		if err := d.load(); err != nil {
			return nil, err
		}
	}

	i, ok := d.entryIndex[name]
	if !ok {
		return nil, fuse.ENOENT
	}

	return d.fs.makeNode(d.entries[i], d.inode)
}

// File represents a file node.
type File struct {
	*attr
	digest stemma.Digest
	rsc    stemma.ReadSeekCloser
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
	rsc, err := f.fs.objects.GetFile(f.digest)
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
