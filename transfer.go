package stemma

import (
	"bytes"
	"fmt"
	"io"
)

type digestSet map[string]struct{}

func (s digestSet) Contains(digest Digest) bool {
	_, contains := s[digest.Hex()]
	return contains
}

func (s digestSet) Add(digest Digest) {
	s[digest.Hex()] = struct{}{}
}

func (s digestSet) Remove(digest Digest) {
	delete(s, digest.Hex())
}

type ProgressMeter struct {
	TransferredObjects uint32
	SkippedObjects     uint32
	TotalObjects       uint32
	TransferredSize    uint64
	SkippedSize        uint64
	TotalSize          uint64
}

type byteCountReader struct {
	io.Reader
	count *uint64
}

func (bcr *byteCountReader) Read(p []byte) (n int, err error) {
	n, err = bcr.Reader.Read(p)
	*bcr.count += uint64(n)
	return n, err
}

func newByteCountReader(r io.Reader, count *uint64) io.Reader {
	return &byteCountReader{
		Reader: r,
		count:  count,
	}
}

type byteCountWriter struct {
	io.Writer
	count *uint64
}

func (bcw *byteCountWriter) Write(p []byte) (n int, err error) {
	n, err = bcw.Writer.Write(p)
	*bcw.count += uint64(n)
	return n, err
}

func newByteCountWriter(w io.Writer, count *uint64) io.Writer {
	return &byteCountWriter{
		Writer: w,
		count:  count,
	}
}

type ReadWriteFlusher interface {
	Read(p []byte) (n int, err error)
	Write(p []byte) (nn int, err error)
	Flush() error
}

type WriteFlusher interface {
	Write(p []byte) (nn int, err error)
	Flush() error
}

// RemoteObjectFetcher represents a session with a remote object store which
// we are fetching objects from.
type RemoteObjectFetcher interface {
	RequestObject(desc Descriptor) error
	SkipObject(desc Descriptor) error
	NextObject(size uint64) io.Reader
	SignalDone() error
}

type descriptorStreamHeader byte

const (
	descriptorStreamHeaderDone descriptorStreamHeader = iota
	descriptorStreamHeaderWant
	descriptorStreamHeaderSkip
)

type descriptorStreamItem struct {
	hdr  descriptorStreamHeader
	desc Descriptor
}

type remoteObjectFetcher struct {
	rwf         ReadWriteFlusher
	err         error
	descriptors chan descriptorStreamItem
}

func newRemoteObjectFetcher(rwf ReadWriteFlusher) RemoteObjectFetcher {
	rof := &remoteObjectFetcher{
		rwf:         rwf,
		descriptors: make(chan descriptorStreamItem, 256),
	}

	// This goroutine waits for new descriptors from the descriptor stream
	// channel and attempts to write them to the underlying writer until
	// the fetcher signals done by closing the channel. If any error
	// occurs while writing, the err value is set on this fetcher object.
	// As this goroutine may block on writing or reading from the
	// descriptor channel, it's important to close the channel and close
	// the underlying writer in case of any external error.
	go func() {
		for {
			next, ok := <-rof.descriptors
			if !ok {
				// Signals that we are done requesting
				// or skipping descriptors.
				return
			}

			if _, err := rof.rwf.Write([]byte{byte(next.hdr)}); err != nil {
				rof.err = fmt.Errorf("unable to write descriptor stream header: %s", err)
				return
			}

			if err := MarshalDescriptor(rof.rwf, next.desc); err != nil {
				rof.err = fmt.Errorf("unable to write next descriptor: %s", err)
				return
			}

			if err := rof.rwf.Flush(); err != nil {
				rof.err = fmt.Errorf("unable to flush descriptor stream: %s", err)
				return
			}
		}
	}()

	return rof
}

func (rof *remoteObjectFetcher) RequestObject(desc Descriptor) error {
	rof.descriptors <- descriptorStreamItem{
		hdr:  descriptorStreamHeaderWant,
		desc: desc,
	}

	return rof.err
}

func (rof *remoteObjectFetcher) SkipObject(desc Descriptor) error {
	rof.descriptors <- descriptorStreamItem{
		hdr:  descriptorStreamHeaderSkip,
		desc: desc,
	}

	return rof.err
}

func (rof *remoteObjectFetcher) NextObject(size uint64) io.Reader {
	return io.LimitReader(rof.rwf, int64(size))
}

func (rof *remoteObjectFetcher) SignalDone() error {
	close(rof.descriptors)

	if _, err := rof.rwf.Write([]byte{byte(descriptorStreamHeaderDone)}); err != nil {
		rof.err = fmt.Errorf("unable to write descriptor stream header: %s", err)
	} else if err := rof.rwf.Flush(); err != nil {
		rof.err = fmt.Errorf("unable to flush descriptor stream: %s", err)
	}

	return rof.err
}

type tempRefDep struct {
	tempRef        TempRef
	numMissingDeps int
}

type dependencySet map[string][]*tempRefDep

// Add adds the given tempRefDep to the list of held parent tempRefDep objects
// for the given child digest key.
func (ds dependencySet) Add(key Digest, dep *tempRefDep) {
	hexKey := key.Hex()
	ds[hexKey] = append(ds[hexKey], dep)
}

// Remove iterates through all of the held tempRefDep objects which depend on
// the given child digest key. The count for each tempRefDep is deceremented.
// If the count goes to zero, that tempRef is committed and Remove is then
// called for that object.
func (ds dependencySet) Remove(key Digest) error {
	hexKey := key.Hex()

	for _, dep := range ds[hexKey] {
		dep.numMissingDeps--
		if dep.numMissingDeps == 0 {
			if _, err := dep.tempRef.Commit(); err != nil {
				return fmt.Errorf("unable to commit object to local store: %s", err)
			}

			if err := ds.Remove(dep.tempRef.Descriptor().Digest()); err != nil {
				return err
			}
		}
	}

	delete(ds, hexKey)

	return nil
}

func (r *Repository) fetchObjects(fetcher RemoteObjectFetcher, desc Descriptor, progress *ProgressMeter) error {
	waitStack := NewDescriptorStack(0)
	inFlightQueue := NewDescriptorQueue(256)
	requestedDigestSet := make(digestSet, 1024)
	objectDeps := make(dependencySet, 1024)

	inFlightQueue.PushBack(desc)
	requestedDigestSet.Add(desc.Digest())
	fetcher.RequestObject(desc)

	for !inFlightQueue.Empty() {
		desc := inFlightQueue.Peek()

		remoteObject := newByteCountReader(fetcher.NextObject(desc.Size()), &progress.TransferredSize)

		tempRef, dependencies, err := r.receiveObject(remoteObject, desc)
		if err != nil {
			return fmt.Errorf("unable to copy remote object %s to local store: %s", desc.Digest().Hex(), err)
		}

		progress.TransferredObjects++

		inFlightQueue.Pop()
		requestedDigestSet.Remove(desc.Digest())

		// Determine the missing dependencies for this object.
		depTracker := &tempRefDep{
			tempRef:        tempRef,
			numMissingDeps: 0, // So far.
		}

		for _, desc := range dependencies {
			queued := requestedDigestSet.Contains(desc.Digest())
			have := !queued && r.Contains(desc.Digest())

			if !have {
				depTracker.numMissingDeps++
				objectDeps.Add(desc.Digest(), depTracker)
			}

			if queued || have {
				// Already waiting to receive or have this object.
				fetcher.SkipObject(desc)
				progress.SkippedObjects += 1 + desc.NumSubObjects()
				progress.SkippedSize += desc.Size() + desc.SubObjectsSize()
				continue
			}

			waitStack.PushFront(desc)
			requestedDigestSet.Add(desc.Digest())
		}

		if depTracker.numMissingDeps == 0 {
			// It's safe to commit the tempRef as it hase no
			// missing dependencies.
			if _, err := tempRef.Commit(); err != nil {
				return fmt.Errorf("unable to commit object to local store: %s", err)
			}

			// Commit any pending objects that were waiting on
			// this one.
			if err := objectDeps.Remove(desc.Digest()); err != nil {
				return err
			}
		}

		for !(inFlightQueue.Full() || waitStack.Empty()) {
			desc := waitStack.Pop()
			inFlightQueue.PushBack(desc)
			fetcher.RequestObject(desc)
		}
	}

	return fetcher.SignalDone()
}

func (r *Repository) receiveObject(remoteObject io.Reader, desc Descriptor) (tempRef TempRef, deps []Descriptor, err error) {
	objWriter, err := r.newObjectWriter(desc.Type())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get new object writer: %s", err)
	}

	defer func() {
		if err != nil {
			objWriter.Cancel()
		}
	}()

	var writer io.Writer = objWriter
	getDeps := func() ([]Descriptor, error) { return nil, nil }

	switch desc.Type() {
	case ObjectTypeApplication, ObjectTypeDirectory:
		objBuf := bytes.NewBuffer(make([]byte, 0, desc.Size()))
		writer = io.MultiWriter(objWriter, objBuf)

		if desc.Type() == ObjectTypeApplication {
			getDeps = func() ([]Descriptor, error) {
				app, err := UnmarshalApplication(objBuf)
				if err != nil {
					return nil, fmt.Errorf("unable to unmarshal application object: %s", err)
				}

				return app.Dependencies(), nil
			}
		} else {
			getDeps = func() ([]Descriptor, error) {
				dir, err := UnmarshalDirectory(objBuf)
				if err != nil {
					return nil, fmt.Errorf("unable to unmarshal directory object: %s", err)
				}

				return dir.Dependencies(), nil
			}
		}
	}

	if _, err := io.Copy(writer, remoteObject); err != nil {
		return nil, nil, fmt.Errorf("unable to copy all bytes from object: %s", err)
	}

	if err := objWriter.Flush(); err != nil {
		return nil, nil, fmt.Errorf("unable to flush object writer: %s", err)
	}

	if !objWriter.Digest().Equals(desc.Digest()) {
		return nil, nil, fmt.Errorf("digest mismatch: %s", objWriter.Digest().Hex())
	}

	// Now that the digest is verified, we can parse the object to get its
	// dependencies.
	deps, err = getDeps()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get object dependencies: %s", err)
	}

	// Do not commit this object into the repository yet as all of its
	// dependencies may not yet be in the repository.
	tempRef, err = objWriter.Hold()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to hold object in temp storage: %s", err)
	}

	return tempRef, deps, nil
}

func (r *Repository) serveObjects(rwf ReadWriteFlusher, progress *ProgressMeter) error {
	digests := make(chan Digest, 256)

	// We can't explicitly cancel the goroutines if they are blocked on a
	// read or write operation. Once the connection is closed, these
	// goroutines will send the error on these channels but we will no
	// longer be waiting to receive on them. Buffering these channels
	// allows the goroutines to place their error in the buffer and exit.
	readDone := make(chan error, 1)
	sendDone := make(chan error, 1)

	go r.sendObjects(rwf, progress, digests, sendDone)
	go readDigests(rwf, progress, digests, readDone)

	select {
	case err := <-readDone:
		if err != nil {
			return fmt.Errorf("unable to read digests from remote: %s", err)
		}

		// The remote is done requesting objects. The reading goroutine
		// will have closed the digests channel, so the sending
		// goroutine will get a nil digest once it drains the channel
		// buffer. Now we wait for the sendObjects goroutine to
		// complete.
		return <-sendDone

	case err := <-sendDone:
		if err == nil {
			// This shouldn't happen. The other goroutine should
			// have completed.
			err = fmt.Errorf("sendObject goroutine quit unexpectedly without an error")
		}

		return fmt.Errorf("unable to send objects to remote: %s", err)
	}
}

// readDigests reads digests from the given reader until the remote signals
// that they are done sending digests at which point the digests channel is
// closed and nil is sent on the done channel. If an error occurs, a non-nil
// error is sent on the done channel. If the digests channel is at capacity,
// rather than block on adding another digest, an error will be sent on the
// done channel. To cancel this goroutine, close the given reader which will
// result in a non-nil error being sent on the done channel, so the done
// channel should either be read from after that or buffered so that this
// goroutine does not block forever.
func readDigests(r io.Reader, progress *ProgressMeter, digests chan<- Digest, done chan<- error) {
	maxDigests := cap(digests)

	for {
		hdrBuf := make([]byte, 1)
		if _, err := r.Read(hdrBuf); err != nil {
			done <- fmt.Errorf("unable to read next digest header: %s", err)
			return
		}

		hdr := descriptorStreamHeader(hdrBuf[0])

		if hdr == descriptorStreamHeaderDone {
			// Signals that the remote is done sending digests.
			close(digests)
			done <- nil
			return
		}

		desc, err := UnmarshalDescriptor(r)
		if err != nil {
			done <- fmt.Errorf("unable to read next descriptor: %s", err)
			return
		}

		if hdr == descriptorStreamHeaderSkip {
			// Remote already has or is waiting for this object.
			progress.SkippedObjects += 1 + desc.NumSubObjects()
			progress.SkippedSize += desc.Size() + desc.SubObjectsSize()
			continue
		}

		if hdr != descriptorStreamHeaderWant {
			done <- fmt.Errorf("unknown descriptor stream header value: %d", hdr)
			return
		}

		if len(digests) >= maxDigests {
			done <- fmt.Errorf("too many digests requested: %d - remote must wait", maxDigests)
			return
		}

		digests <- desc.Digest()
	}
}

// sendObjects waits to receive digests from the given digest channel. The
// object for the digest is then copied from this repository to the given
// connection. If a nil digest is read from the digests channel (such as when
// the channel has been closed and drained), a nil error will be sent on the
// done channel and the function will return. If any error occurs, a non-nil
// error will be sent on the done channel. To cancel this goroutine, close the
// given writer which will result in a non-nil error being sent on the done
// channel, so the done channel should either be read from after that or
// buffered so that this goroutine does not block forever.
func (r *Repository) sendObjects(wf WriteFlusher, progress *ProgressMeter, digests <-chan Digest, done chan<- error) {
	for {
		digest := <-digests
		if digest == nil {
			// No more digests to process.
			done <- nil
			return
		}

		if err := r.sendObject(wf, progress, digest); err != nil {
			done <- err
			return
		}
	}
}

func (r *Repository) sendObject(wf WriteFlusher, progress *ProgressMeter, digest Digest) error {
	object, err := r.getObjectFile(digest)
	if err != nil {
		return fmt.Errorf("unable to get object: %s", err)
	}

	defer object.Close()

	// Strip off the object type header first. The remote will write the
	// expected object type header on its side to verify the type and
	// contents of the object.
	if _, err := UnmarshalObjectType(object); err != nil {
		return err
	}

	writer := newByteCountWriter(wf, &progress.TransferredSize)
	if _, err := io.Copy(writer, object); err != nil {
		return fmt.Errorf("unable to copy object: %s", err)
	}

	if err := wf.Flush(); err != nil {
		return fmt.Errorf("unable to flush object: %s", err)
	}

	progress.TransferredObjects++

	return nil
}
