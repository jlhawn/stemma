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
		fmt.Println("Usage: stemma-storedir PATH")
		os.Exit(1)
	}

	repo := stemma.NewRepository(".")

	targetDir := flag.Arg(0)
	objDesc, err := repo.StoreDirectory(targetDir)
	if err != nil {
		log.Fatalf("unable to store directory %q: %s", targetDir, err)
	}

	header, err := stemma.NewHeader(targetDir)
	if err != nil {
		log.Fatalf("unable to make target directory header: %s", err)
	}

	hdrDesc, err := repo.PutHeader(header)
	if err != nil {
		log.Fatalf("unable to store target directory header: %s", err)
	}

	entry := stemma.DirectoryEntry{
		Name:           "/",
		Type:           stemma.DirentTypeDirectory,
		HeaderDigest:   hdrDesc.Digest(),
		HeaderSize:     hdrDesc.Size(),
		ObjectDigest:   objDesc.Digest(),
		ObjectSize:     objDesc.Size(),
		NumSubObjects:  objDesc.NumSubObjects(),
		SubObjectsSize: objDesc.SubObjectsSize(),
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
