package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jlhawn/stemma"
	"github.com/sethgrid/multibar"
)

func main() {
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("Usage: stemma-fetch REMOTE TAG")
		os.Exit(1)
	}

	repo, err := stemma.NewRepository(".")
	if err != nil {
		log.Fatalf("unable to initialize repository: %s", err)
	}

	remote, err := repo.RemoteObjectStore(flag.Arg(0))
	if err != nil {
		log.Fatalf("unable to get remote object store: %s", err)
	}

	ref := flag.Arg(1)
	desc, err := remote.GetTag(ref)
	if err != nil {
		log.Fatalf("unable to resolve remote reference: %s", err)
	}

	progress := &stemma.ProgressMeter{
		TotalObjects: 1 + desc.NumSubObjects(),
		TotalSize:    desc.Size() + desc.SubObjectsSize(),
	}

	progBars, err := multibar.New()
	if err != nil {
		log.Fatalf("unable to initialize progress bars: %s", err)
	}

	numObjectsProgressBar := progBars.MakeBar(int(progress.TotalObjects), "Total Objects")
	objectSizeProgressBar := progBars.MakeBar(int(progress.TotalSize), "Total Size   ")

	go progBars.Listen()

	var done bool
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		var numObjects, size int

		for {
			numObjectsProgressBar(numObjects)
			objectSizeProgressBar(size)

			if done {
				break
			}

			time.Sleep(100 * time.Millisecond)

			numObjects = int(progress.SkippedObjects + progress.TransferredObjects)
			size = int(progress.SkippedSize + progress.TransferredSize)
		}

		wg.Done()
	}()

	if err := remote.Fetch(desc, progress); err != nil {
		log.Fatalf("unable to fetch from remote: %s", err)
	}

	done = true
	wg.Wait()

	if err := repo.TagStore().Set(ref, desc); err != nil {
		log.Fatalf("unable to set local tag: %s", err)
	}
}
