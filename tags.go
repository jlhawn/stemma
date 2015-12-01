package stemma

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

// Common errors.
var (
	ErrNoSuchTag  = errors.New("no such tag")
	ErrinvalidTag = errors.New("invalid tag")
)

var validTagPatern = regexp.MustCompile(`^[\w][\w.-]{0,127}$`)

type tagStore struct {
	root string
}

// NewTagStore creates a new tag store using the given root directory.
func NewTagStore(root string) (TagStore, error) {
	if fi, err := os.Stat(root); err != nil {
		return nil, fmt.Errorf("unable to stat directory %q: %s", root, err)
	} else if !fi.IsDir() {
		return nil, fmt.Errorf("unable to use directory %q: not a directory", root)
	}

	return &tagStore{root: root}, nil
}

func (s *tagStore) getPath(tag string) string {
	return filepath.Join(s.root, tag)
}

func (s *tagStore) Get(tag string) (Descriptor, error) {
	descObj, err := os.Open(s.getPath(tag))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoSuchTag
		}

		return nil, fmt.Errorf("unable to open tag file: %s", err)
	}
	defer descObj.Close()

	return UnmarshalDescriptor(descObj)
}

func (s *tagStore) Set(tag string, desc Descriptor) error {
	if !validTagPatern.MatchString(tag) {
		return ErrinvalidTag
	}

	descObj, err := os.OpenFile(s.getPath(tag), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0600))
	if err != nil {
		return fmt.Errorf("unable to open tag file: %s", err)
	}
	defer descObj.Close()

	return MarshalDescriptor(descObj, desc)
}

func (s *tagStore) List() (tags []string, err error) {
	refsDir, err := os.Open(s.root)
	if err != nil {
		return nil, fmt.Errorf("unable to open tags directory: %s", err)
	}

	return refsDir.Readdirnames(0)
}

func (s *tagStore) Remove(tag string) error {
	return os.Remove(s.getPath(tag))
}

// ResolveRef resolves the given reference string (either a tag or a hex-
// encoded digest) to a valid digest.
func (r *Repository) ResolveRef(ref string) (Digest, error) {
	desc, err := r.TagStore().Get(ref)
	if err != nil {
		if err != ErrNoSuchTag {
			return nil, fmt.Errorf("unable to lookup tag: %s", err)
		}

		// Fallback to parsing the ref as a digest.
		return ParseDigest(ref)
	}

	return desc.Digest(), nil
}

func MarshalTagDescriptors(w io.Writer, tagDescriptors map[string]Descriptor) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(tagDescriptors))); err != nil {
		return fmt.Errorf("unable to encode length of tag descriptors: %s", err)
	}

	for tag, desc := range tagDescriptors {
		tagBytes := []byte(tag)
		if err := binary.Write(w, binary.LittleEndian, uint32(len(tagBytes))); err != nil {
			return fmt.Errorf("unable to encode length of tag bytes: %s", err)
		}

		if _, err := w.Write(tagBytes); err != nil {
			return fmt.Errorf("unable to write tag bytes: %s", err)
		}

		if err := MarshalDescriptor(w, desc); err != nil {
			return fmt.Errorf("unable to encode descriptor: %s", err)
		}
	}

	return nil
}

func UnmarshalTagDescriptors(r io.Reader) (tagDescriptors map[string]Descriptor, err error) {
	var numTags uint32
	if err := binary.Read(r, binary.LittleEndian, &numTags); err != nil {
		return nil, fmt.Errorf("unable to decode length of tag descriptors: %s", err)
	}

	tagDescriptors = make(map[string]Descriptor, numTags)

	for i := uint32(0); i < numTags; i++ {
		var numTagBytes uint32
		if err := binary.Read(r, binary.LittleEndian, &numTagBytes); err != nil {
			return nil, fmt.Errorf("unable to decode length of tag bytes: %s", err)
		}

		tagBuf := make([]byte, numTagBytes)
		if _, err := io.ReadFull(r, tagBuf); err != nil {
			return nil, fmt.Errorf("unable to read tag bytes: %s", err)
		}

		desc, err := UnmarshalDescriptor(r)
		if err != nil {
			return nil, fmt.Errorf("unable to decode descriptor: %s", err)
		}

		tagDescriptors[string(tagBuf)] = desc
	}

	return tagDescriptors, nil
}
