package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jlhawn/stemma"
	"github.com/jlhawn/stemma/sysutil"
)

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Usage: stemma-show-app DIGEST|TAG")
		os.Exit(1)
	}

	repo, err := stemma.NewRepository(".")
	if err != nil {
		log.Fatalf("unable to initialize repository: %s", err)
	}

	// Acquire a shared lock on the repository so we can freely read its
	// contents.
	if err := repo.SharedLock(); err != nil {
		log.Fatalf("unable to acquire exclusive repo lock: %s", err)
	}
	defer repo.Unlock()

	appDigest, err := repo.ResolveRef(flag.Arg(0))
	if err != nil {
		log.Fatalf("unable to resolve reference: %s", err)
	}

	app, err := repo.GetApplication(appDigest)
	if err != nil {
		log.Fatalf("unable to get application: %s", err)
	}

	prettyPrintApp(repo, app)
}

func prettyPrintApp(repo *stemma.Repository, app stemma.Application) {
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

	prettyPrint(repo, entry, "/", "")
}

func prettyPrint(repo *stemma.Repository, entry stemma.DirectoryEntry, dirPath, indent string) {
	header, err := repo.GetHeader(entry.HeaderDigest)
	if err != nil {
		log.Fatalf("unable to get object header: %s", err)
	}

	fmt.Printf("%sPath: %s\n", indent, filepath.Join(dirPath, entry.Name))
	fmt.Printf("%sHeader:\n", indent)
	fmt.Printf("%s  Digest:        %s\n", indent, entry.HeaderDigest)
	fmt.Printf("%s  Header Size:   %d\n", indent, entry.HeaderSize)
	fmt.Printf("%s  Mode:          %s\n", indent, header.Mode)
	fmt.Printf("%s  Rdev:          %x\n", indent, header.Rdev)
	fmt.Printf("%s  UID:           %d\n", indent, header.UID)
	fmt.Printf("%s  GID:           %d\n", indent, header.GID)
	fmt.Printf("%s  Xattrs: {\n", indent)

	for _, xattr := range sysutil.NewXattrs(header.Xattrs) {
		fmt.Printf("%s    %s -> %q\n", indent, xattr.Key, string(xattr.Val))
	}

	fmt.Printf("%s  }\n", indent)

	switch entry.Type {
	case stemma.DirentTypeLink:
		// Print Symlink value and nothing else.
		fmt.Printf("%sLink Target: %s\n", indent, entry.LinkTarget)
		return
	case stemma.DirentTypeRegular, stemma.DirentTypeDirectory:
		// Continue.
	default:
		// Not a regular file, symlink, or directory.
		return
	}

	objDesc := entry.ObjectDescriptor()

	fmt.Printf("%sObject:\n", indent)
	fmt.Printf("%s  Type:                 %s\n", indent, objDesc.Type())
	fmt.Printf("%s  Digest:               %s\n", indent, objDesc.Digest())
	fmt.Printf("%s  Object Size:          %d\n", indent, objDesc.Size())

	if entry.Type == stemma.DirentTypeRegular {
		// No other information to display for regular files.
		return
	}

	fmt.Printf("%s  Subobject Count:      %d\n", indent, objDesc.NumSubObjects())
	fmt.Printf("%s  Total Subobject Size: %d\n", indent, objDesc.SubObjectsSize())

	dir, err := repo.GetDirectory(entry.ObjectDigest)
	if err != nil {
		log.Fatalf("unable to get directory object: %s", err)
	}

	fmt.Printf("%sEntries: [\n", indent)
	for i, subEntry := range dir {
		if i > 0 {
			fmt.Println()
		}

		prettyPrint(repo, subEntry, filepath.Join(dirPath, entry.Name), indent+"    ")
	}
	fmt.Printf("%s]\n", indent)
}
