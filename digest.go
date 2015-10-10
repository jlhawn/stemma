package stemma

import (
	"crypto"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	// Imported to register hash functions.
	_ "crypto/sha256"
	_ "crypto/sha512"
	_ "golang.org/x/crypto/sha3"
)

// Common Digest Errors.
var (
	ErrInvalidDigestFormat = errors.New("invalid digest format")
	ErrInvalidDigestAlg    = errors.New("invalid digest algorithm")
)

// DigestAlg is used to specify a digest algorithm. 255 possibilities should be
// enough to last us until the robots take over. The digest will be encoded to
// hex and prepended to hex digest strings (e.g., all SHA256 digests will
// begin with "01...", all SHA512_256 with "05..."). The remainder of the
// digest string will be the hex encoded digest of corresponding length.
type DigestAlg byte

// Digest algorithms.
const (
	DigestAlgSHA224 DigestAlg = iota
	DigestAlgSHA256
	DigestAlgSHA384
	DigestAlgSHA512
	DigestAlgSHA512_224
	DigestAlgSHA512_256
	DigestAlgSHA3_224
	DigestAlgSHA3_256
	DigestAlgSHA3_384
	DigestAlgSHA3_512

	DigestAlgUnknown = 255
)

type digestAlgInfo struct {
	name       string
	cryptoHash crypto.Hash
}

// registeredDigestAlgs holds the names of registered digest algorithms.
var registeredDigestAlgs = map[DigestAlg]digestAlgInfo{
	DigestAlgSHA224:     {"SHA224", crypto.SHA224},
	DigestAlgSHA256:     {"SHA256", crypto.SHA256},
	DigestAlgSHA384:     {"SHA384", crypto.SHA384},
	DigestAlgSHA512:     {"SHA512", crypto.SHA512},
	DigestAlgSHA512_224: {"SHA512_224", crypto.SHA512_224},
	DigestAlgSHA512_256: {"SHA512_256", crypto.SHA512_256},
	DigestAlgSHA3_224:   {"SHA3_224", crypto.SHA3_224},
	DigestAlgSHA3_256:   {"SHA3_256", crypto.SHA3_256},
	DigestAlgSHA3_384:   {"SHA3_384", crypto.SHA3_384},
	DigestAlgSHA3_512:   {"SHA3_512", crypto.SHA3_512},
}

func (a DigestAlg) String() string {
	info, ok := registeredDigestAlgs[a]
	if !ok {
		return "Unknown"
	}

	return info.name
}

// Digest is a byte sum from a cryptographic hash function tagged with an
// algorithm identifier prefix byte corresponding to one of the above Digest
// Algorithms.
type Digest []byte

// Algorithm returns the algorithm identifier for this digest.
func (d Digest) Algorithm() DigestAlg {
	if len(d) == 0 {
		return DigestAlgUnknown
	}

	// Should be encoded in the first byte.
	alg := DigestAlg(d[0])
	if int(alg) >= len(registeredDigestAlgs) {
		return DigestAlgUnknown
	}

	return alg
}

// Bytes returns a copy of the raw bytes of this digest (the first byte is the
// 1-byte algorithm identifier).
func (d Digest) Bytes() []byte {
	buf := make([]byte, len(d))
	copy(buf, []byte(d))
	return buf
}

// Hex returns the hexadecimal encoding of this digest.
func (d Digest) Hex() string {
	return hex.EncodeToString([]byte(d))
}

func (d Digest) String() string {
	return d.Hex()
}

// ParseDigest parses a new digest from the given hexadecimal digest string.
func ParseDigest(dgst string) (Digest, error) {
	buf, err := hex.DecodeString(dgst)
	if err != nil {
		return nil, fmt.Errorf("unable to decode digest hex: %s", err)
	}

	return Digest(buf), nil
}

// Marshal marshals this digest into the given writer.
func (d Digest) Marshal(w io.Writer) error {
	return marshalBytes(w, d.Bytes())
}

// UnmarshalDigest unmarshals a digest from the given reader.
func UnmarshalDigest(r io.Reader) (Digest, error) {
	buf, err := unmarshalBytes(r)
	if err != nil {
		return nil, fmt.Errorf("unable to decode digest buffer: %s", err)
	}

	return Digest(buf), nil
}

// Digester is used to generate a digest from hashing raw object data.
type Digester interface {
	hash.Hash
	Algorithm() DigestAlg
	Digest() Digest
}

type digester struct {
	alg DigestAlg
	hash.Hash
}

// NewDigester returns a new Digester using the given algorithm. If the given
// algorithm is not supported the returned error will be ErrInvalidDigestAlg.
func NewDigester(alg DigestAlg) (Digester, error) {
	algInfo, ok := registeredDigestAlgs[alg]
	if !ok {
		return nil, ErrInvalidDigestAlg
	}

	return &digester{
		alg:  alg,
		Hash: algInfo.cryptoHash.New(),
	}, nil
}

func (d digester) Algorithm() DigestAlg {
	return d.alg
}

func (d digester) Digest() Digest {
	return Digest(append([]byte{byte(d.alg)}, d.Hash.Sum(nil)...))
}
