package blobstore

import (
	"errors"
	"fmt"
	"hash"
	"io"
	"net/url"
	"os"
	"path"

	log "github.com/maerics/golog"
)

type filesystemBlobstore struct {
	basedir   string
	newhashfn func() hash.Hash
}

func (f *filesystemBlobstore) initialize(c Config) error {
	if u, err := url.Parse(c.URL); err != nil {
		return err
	} else {
		switch true {
		case u.Host != "" && u.Path != "":
			f.basedir = path.Join(u.Host, u.Path)
		case u.Host != "":
			f.basedir = u.Host
		case u.Path != "":
			f.basedir = u.Path
		default:
			return fmt.Errorf("invalid file url %q is missing path", c.URL)
		}
	}
	f.newhashfn = c.newhash
	return os.MkdirAll(f.basedir, os.FileMode(0o700)) // TODO: pass filemode option?
}

func (f *filesystemBlobstore) Fetch(name string) (io.ReadCloser, error) {
	r, err := os.Open(path.Join(f.basedir, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return r, err
}

func (f *filesystemBlobstore) Store(r io.ReadCloser) (string, error) {
	// Read the source into a tempfile.
	tempfile, err := os.CreateTemp("", "blob-")
	if err != nil {
		return "", err
	}
	hash := f.newhashfn()
	w := io.MultiWriter(tempfile, hash)
	if _, err := io.Copy(w, r); err != nil {
		return "", err
	}
	if err := tempfile.Close(); err != nil {
		return "", fmt.Errorf("failed to close tempfile: %w", err)
	}
	if err := r.Close(); err != nil {
		return "", fmt.Errorf("failed to close source reader: %w", err)
	}
	filename := fmt.Sprintf("%x", hash.Sum(nil))

	// Move the file to its correct name, if it doesn't already exist.
	fullpath := path.Join(f.basedir, filename)
	if _, err := os.Stat(fullpath); errors.Is(err, os.ErrNotExist) {
		if err := os.Link(tempfile.Name(), fullpath); err != nil {
			return "", fmt.Errorf("failed to rename object: %w", err)
		}
	} else {
		log.Fatalf("%v", err)
	}
	os.Remove(tempfile.Name())

	// Return the name.
	return filename, nil
}
