package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/jlhawn/stemma"
)

func main() {
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("Usage: stemma-storedir PATH TAG")
		os.Exit(1)
	}

	repo, err := stemma.NewRepository(".")
	if err != nil {
		log.Fatalf("unable to initialize repository: %s", err)
	}

	// Acquire an exclusive lock on the repository as we will be adding
	// a new tag.
	if err := repo.ExclusiveLock(); err != nil {
		log.Fatalf("unable to acquire exclusive repo lock: %s", err)
	}
	defer repo.Unlock()

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

	a := stemma.Application{
		Rootfs: stemma.Rootfs{
			Header: stemma.RootfsHeader{
				Digest: hdrDesc.Digest(),
				Size:   hdrDesc.Size(),
			},
			Directory: stemma.RootfsDirectory{
				Digest:         objDesc.Digest(),
				Size:           objDesc.Size(),
				NumSubObjects:  objDesc.NumSubObjects(),
				SubObjectsSize: objDesc.SubObjectsSize(),
			},
		},
	}

	appDesc, err := repo.PutApplication(a)
	if err != nil {
		log.Fatalf("unable to store application object: %s", err)
	}

	if err := repo.TagStore().Set(flag.Arg(1), appDesc); err != nil {
		log.Fatalf("unable to set tag: %s", err)
	}

	fmt.Printf("Application:\n")
	fmt.Printf("  Digest:               %s\n", appDesc.Digest())
	fmt.Printf("  Size:                 %d\n", appDesc.Size())
	fmt.Printf("  Subobject Count:      %d\n", appDesc.NumSubObjects())
	fmt.Printf("  Total Subobject Size: %d\n", appDesc.SubObjectsSize())
}
