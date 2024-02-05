package main

import (
	"blobstore"
	"fmt"
	"io"
	"os"

	log "github.com/maerics/golog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config struct {
	Dirname  string `yaml:"dirname"`
	HashFunc string `yaml:"hashfunc"`
}

func main() {
	cmd := &cobra.Command{
		Use:   "blobstore",
		Short: "Fetch and store byte sequences named by a hash",

		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}

	config := GetConfig()
	cmd.PersistentFlags().StringVarP(&config.Dirname, "dirname", "d", config.Dirname, "filesystem directory name")
	cmd.PersistentFlags().StringVarP(&config.HashFunc, "hashfunc", "f", config.HashFunc, "hash function name")

	getBlobstore := func() blobstore.Blobstore {
		config := blobstore.Config{
			URL:  fmt.Sprintf("file://%v", config.Dirname),
			Hash: config.HashFunc,
		}

		bs, err := blobstore.New(config)
		if err != nil {
			log.Fatalf("%v", err)
		}
		return bs
	}

	checkConfig := func(c *cobra.Command, args []string) error {
		if config.Dirname == "" {
			return fmt.Errorf(`"dirname" is required but none was given`)
		}
		if config.HashFunc == "" {
			return fmt.Errorf(`"hashfunc" is required but none was given`)
		}
		return nil
	}

	cmd.AddCommand(&cobra.Command{
		Use:     "store",
		Aliases: []string{"put", "save", "write"},
		Short:   "Store the bytes from stdin",
		Args:    cobra.ExactArgs(0),
		PreRunE: checkConfig,
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
		PreRunE: checkConfig,
		Run: func(cmd *cobra.Command, args []string) {
			r := log.Must1(getBlobstore().Fetch(args[0]))
			log.Must1(io.Copy(os.Stdout, r))
			log.Must(r.Close())
		},
	})

	cmd.Execute()
}

func GetConfig() Config {
	viper.SetConfigName(".blobstore")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME")

	err := viper.ReadInConfig()
	if err != nil {
		log.Debugf("no config file found: %v", err)
	}

	return Config{
		Dirname:  viper.GetString("dirname"),
		HashFunc: viper.GetString("hashfunc"),
	}
}
