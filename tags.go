package stemma

import (
	"errors"
	"fmt"
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

	return unmarshalDescriptor(descObj)
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

	return marshalDescriptor(descObj, desc)
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
