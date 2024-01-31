package main

import (
	"blobstore"
	"fmt"
	"io"
	"os"

	log "github.com/maerics/golog"
	"github.com/spf13/cobra"
)

func main() {
	var dirname string = "/var/db/blobs"
	var hashname string = "sha1"

	cmd := &cobra.Command{
		Use:   "blobstore",
		Short: "Fetch and store byte sequences named by a hash",

		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}

	cmd.PersistentFlags().StringVarP(&dirname, "dirname", "d", dirname, "filesystem directory name")
	cmd.PersistentFlags().StringVarP(&hashname, "hash", "", hashname, "hash function name")

	getBlobstore := func() blobstore.Blobstore {
		config := blobstore.Config{
			URL:  fmt.Sprintf("file://%v", dirname),
			Hash: hashname,
		}

		bs, err := blobstore.New(config)
		if err != nil {
			log.Fatalf("%v", err)
		}
		return bs
	}

	cmd.AddCommand(&cobra.Command{
		Use:     "store",
		Aliases: []string{"put", "save", "write"},
		Short:   "Store the bytes from stdin",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if name, err := getBlobstore().Store(os.Stdin); err != nil {
				log.Fatalf("%v", err)
			} else {
				fmt.Println(name)
			}
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:     "fetch",
		Aliases: []string{"get", "read"},
		Short:   "Fetch the named object from storage",
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			r, err := getBlobstore().Fetch(args[0])
			if err != nil {
				log.Fatalf("%v", err)
			}
			log.Must1(io.Copy(os.Stdout, r))
			log.Must(r.Close())
		},
	})

	cmd.Execute()
}
