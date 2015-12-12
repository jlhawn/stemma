package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jlhawn/stemma"
)

func main() {
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("Usage: stemma-push REMOTE TAG")
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
	desc, err := repo.TagStore().Get(ref)
	if err != nil {
		log.Fatalf("unable to resolve reference: %s", err)
	}

	progress := &stemma.ProgressMeter{
		TotalObjects: 1 + desc.NumSubObjects(),
		TotalSize:    desc.Size() + desc.SubObjectsSize(),
	}

	fmt.Printf("Total Objects: %10d %6s\n", progress.TotalObjects, humanSize(progress.TotalSize))

	done := make(chan int)
	go func() {
		for {
			select {
			case <-done:
				updateProgress(progress)
				done <- 1
				return
			default:
				updateProgress(progress)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	if err := remote.Push(desc, progress); err != nil {
		log.Fatalf("unable to push to remote: %s", err)
	}

	done <- 1
	<-done

	fmt.Printf("\nSkipped Objects: %10d %6s\n", progress.SkippedObjects, humanSize(progress.SkippedSize))
}

func updateProgress(progress *stemma.ProgressMeter) {
	objectsProgress := uint64(progress.TransferredObjects + progress.SkippedObjects)
	sizeProgress := progress.TransferredSize + progress.SkippedSize
	fmt.Printf(
		"\rTransferring Objects: %6d %6.2f%%  %10s %6.2f%%",
		objectsProgress, percent(objectsProgress, uint64(progress.TotalObjects)),
		humanSize(sizeProgress), percent(sizeProgress, progress.TotalSize),
	)
}

func percent(current, total uint64) float64 {
	return float64(current) / float64(total) * 100.0
}

const (
	kilobyte = 1024
	megabyte = kilobyte * kilobyte
	gigabyte = megabyte * kilobyte
	terabyte = megabyte * kilobyte
)

func humanSize(numBytes uint64) string {
	switch {
	case numBytes > terabyte:
		return fmt.Sprintf("%.3fTB", float64(numBytes)/terabyte)
	case numBytes > gigabyte:
		return fmt.Sprintf("%.3fGB", float64(numBytes)/gigabyte)
	case numBytes > megabyte:
		return fmt.Sprintf("%.3fMB", float64(numBytes)/megabyte)
	case numBytes > kilobyte:
		return fmt.Sprintf("%.3fKB", float64(numBytes)/kilobyte)
	default:
		return fmt.Sprintf("%dB", numBytes)
	}
}
