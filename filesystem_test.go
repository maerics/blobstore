package blobstore

import (
	"fmt"
	"testing"
)

func TestFilesystemInit(t *testing.T) {
	for _, example := range []struct{ url, basedir string }{
		{"file:///tmp", "/tmp"},
		{"file://data", "data"},
		{"file://./data", "data"},
		{"file://data/storage", "data/storage"},
	} {
		f := &filesystemBlobstore{}
		err := f.initialize(Config{URL: example.url})
		if err != nil {
			t.Fatal(err)
		} else if f.basedir != example.basedir {
			t.Errorf("wanted basedir %q for config url %q, got %q", example.basedir, example.url, f.basedir)
		}
	}
}

func TestFilesystemInitInvalidURL(t *testing.T) {
	const invalidUrl = "file://"

	f := &filesystemBlobstore{}
	err := f.initialize(Config{URL: invalidUrl})
	expected := fmt.Sprintf(`invalid file url %q is missing path`, invalidUrl)
	if err == nil || err.Error() != expected {
		t.Errorf("unexpected error for invalid url: %q", err)
	}
}
