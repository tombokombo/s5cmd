package command

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/log/stat"
	"github.com/peak/s5cmd/parallel"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

var selectHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] argument

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	01. Search for all JSON objects with the foo property set to 'bar' and spit them into stdout
		 > s5cmd {{.HelpName}} --compression-type gzip --query "SELECT * FROM S3Object s WHERE s.foo='bar'" s3://bucket/*
`

var selectCommandFlags = []cli.Flag{
	&cli.StringFlag{
		Name:    "query",
		Aliases: []string{"e"},
		Usage:   "SQL expression to use to select from the objects",
	},
	&cli.StringFlag{
		Name:  "compression-type",
		Usage: "Type of compression used in storage",
	},
}

var selectCommand = &cli.Command{
	Name:               "select",
	HelpName:           "select",
	Usage:              "select objects containing JSON using a SQL query",
	Flags:              selectCommandFlags,
	CustomHelpTemplate: selectHelpTemplate,
	Before: func(c *cli.Context) error {
		err := validateSelectCommand(c)
		if err != nil {
			printError(givenCommand(c), c.Command.Name, err)
		}
		return err
	},
	Action: func(c *cli.Context) (err error) {
		defer stat.Collect(c.Command.FullName(), &err)()

		return Select{
			src:         c.Args().Get(0),
			op:          c.Command.Name,
			fullCommand: givenCommand(c),
			// flags
			query:           c.String("query"),
			compressionType: c.String("compression-type"),

			storageOpts: NewStorageOpts(c),
		}.Run(c.Context)
	},
}

// Select holds select operation flags and states.
type Select struct {
	src         string
	op          string
	fullCommand string

	deleteSource bool

	query           string
	compressionType string

	// s3 options
	storageOpts storage.Options
}

// Run starts copying given source objects to destination.
func (s Select) Run(ctx context.Context) error {
	srcurl, err := url.New(s.src)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	client, err := storage.NewRemoteClient(ctx, srcurl, s.storageOpts)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	objch, err := expandSource(ctx, client, false, srcurl)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	var merror error

	waiter := parallel.NewWaiter()
	errDoneCh := make(chan bool)

	go func() {
		defer close(errDoneCh)
		for err := range waiter.Err() {
			printError(s.fullCommand, s.op, err)
			merror = multierror.Append(merror, err)
		}
	}()

	for object := range objch {
		if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			printError(s.fullCommand, s.op, err)
			continue
		}

		if object.StorageClass.IsGlacier() {
			err := fmt.Errorf("object '%v' is on Glacier storage", object)
			printError(s.fullCommand, s.op, err)
			continue
		}

		task := s.prepareTask(ctx, client, object.URL)
		parallel.Run(task, waiter)
	}

	waiter.Wait()
	<-errDoneCh

	return merror
}

func (s Select) prepareTask(ctx context.Context, client *storage.S3, url *url.URL) func() error {
	return func() error {
		query := &storage.SelectQuery{
			ExpressionType:  "SQL",
			Expression:      s.query,
			CompressionType: s.compressionType,
		}

		rc, err := client.Select(ctx, url, query, os.Stdout)
		if err != nil {
			return err
		}
		defer rc.Close()

		_, err = io.Copy(os.Stdout, rc)
		return err
	}
}

func validateSelectCommand(c *cli.Context) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("expected source argument")
	}

	src := c.Args().Get(0)

	srcurl, err := url.New(src)
	if err != nil {
		return err
	}

	if !srcurl.IsRemote() {
		return fmt.Errorf("source must be remote")
	}

	return nil
}
