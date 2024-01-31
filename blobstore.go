package blobstore

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"strings"
)

type Blobstore interface {
	initialize(Config) error

	// Return a read stream for the named object, or nil if not found, or an error.
	Fetch(string) (io.ReadCloser, error)

	// Store the bytes provided by the given reader and return the name.
	Store(r io.ReadCloser) (string, error)
}

const (
	// AwsS3Scheme              = "s3"
	FileScheme = "file"
	// GoogleCloudStorageScheme = "gcs"
)

type Config struct {
	URL     string `json:"url"`
	Hash    string `json:"hash"`
	newhash func() hash.Hash
}

var hashfns = map[string]func() hash.Hash{
	"md5":    func() hash.Hash { return md5.New() },
	"sha1":   func() hash.Hash { return sha1.New() },
	"sha256": func() hash.Hash { return sha256.New() },
	"sha512": func() hash.Hash { return sha512.New() },
}

func New(config Config) (Blobstore, error) {
	// Ensure that the configuration is valid.
	if newHashfn, present := hashfns[config.Hash]; !present {
		return nil, fmt.Errorf("invalid hash function name %q", config.Hash)
	} else {
		config.newhash = newHashfn
	}

	// Create, initialize, and return the blobstore.
	var b Blobstore
	switch true {
	case strings.HasPrefix(config.URL, FileScheme+":"):
		b = &filesystemBlobstore{}
	default:
		return nil, fmt.Errorf("unhandled blobstore URL %q", config.URL)
	}
	if err := b.initialize(config); err != nil {
		return nil, err
	}
	return b, nil
}
