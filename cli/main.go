package main

import (
	"os"
	"runtime/pprof"
	"strconv"

	"io"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"encoding/json"

	"github.com/pjvds/randombytes"
	"github.com/pjvds/streamsdb/storage"
	"github.com/urfave/cli"
	"go.uber.org/zap"
)

func main() {
	log := zap.NewNop()

	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	streamdb := storage.OpenFdb(db, log)

	app := cli.NewApp()
	app.Name = "sdb-cli"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "cpuprofile",
			Usage: "write cpu profile to `file`",
		},
	}
	app.Before = func(c *cli.Context) error {
		if path := c.String("cpuprofile"); len(path) > 0 {
			f, err := os.Create(path)
			if err != nil {
				println("could not create CPU profile: ", err.Error())
				os.Exit(1)
			}
			if err := pprof.StartCPUProfile(f); err != nil {
				println("could not start CPU profile: ", err.Error())
				os.Exit(1)
			}
		}
		return nil
	}
	app.After = func(c *cli.Context) error {
		pprof.StopCPUProfile()
		return nil
	}
	app.Commands = []cli.Command{
		{
			Name: "append",
			Action: func(c *cli.Context) error {
				var rawJson json.RawMessage
				stream := storage.StreamId(c.Args().First())
				decoder := json.NewDecoder(os.Stdin)
				for {
					if err := decoder.Decode(&rawJson); err != nil {
						if err == io.EOF {
							return nil
						}
						println(err.Error())
						return nil
					}
					if _, err := streamdb.Append(stream, storage.Message{
						Payload: rawJson,
					}); err != nil {
						println(err.Error())
						return nil
					}
				}
			},
		},
		{
			Name: "read",
			Action: func(c *cli.Context) error {
				stream := storage.StreamId(c.Args().First())

				position := storage.StreamPosition(1)
				result, err := streamdb.Read(stream, position, 50)
				for err == nil && len(result.Messages) > 0 {
					for _, msg := range result.Messages {
						println(string(msg.Payload))
					}
					if !result.HasNext {
						return nil
					}
					position = result.Next
					result, err = streamdb.Read(stream, position, 50)
				}
				if err != nil {
					println(err.Error())
				}
				return nil
			},
		},
		{
			Name: "write-benchmark",
			Action: func(c *cli.Context) error {
				payload := randombytes.Make(250)

				errors := make(chan error)
				for i := 0; i < 1000; i++ {
					go func(i int) {
						stream := storage.StreamId(c.Args().First() + strconv.Itoa(i))
						messages := []storage.Message{
							{Payload: payload},
							{Payload: payload},
							{Payload: payload},
							{Payload: payload},
						}

						for {
							_, err := streamdb.Append(stream, messages...)
							if err != nil {
								errors <- err
							}
						}
					}(i)
				}

				err := <-errors
				println("FAILED", err)
				return nil
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		println(err)
	}
}
