package main

import (
	"os"
	"strconv"
	"time"

	"io"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/paulbellamy/ratecounter"

	//"github.com/francoispqt/gojay"
	"encoding/json"

	"github.com/pjvds/streamsdb/storage"
	"github.com/urfave/cli"
	"go.uber.org/zap"
)

func main() {
	//log, err := zap.NewDevelopmentConfig().Build()
	//if err != nil {
	//	println(err)
	//	return
	//}
	log := zap.NewNop()

	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	streamdb := storage.OpenFdb(db, log)

	app := cli.NewApp()
	app.Name = "sdb-cli"
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
				counter := ratecounter.NewRateCounter(1 * time.Second)

				errors := make(chan error)
				for i := 0; i < 1000; i++ {
					go func(i int) {
						stream := storage.StreamId(c.Args().First() + strconv.Itoa(i))
						for {
							_, err := streamdb.Append(stream,
								storage.Message{Payload: []byte("hello-world")},
								storage.Message{Payload: []byte("hello-world")},
								storage.Message{Payload: []byte("hello-world")},
								storage.Message{Payload: []byte("hello-world")},
								storage.Message{Payload: []byte("hello-world")},
							)
							if err != nil {
								errors <- err
							}

							counter.Incr(5)
						}
					}(i)
				}

				for {
					select {
					case <-time.After(1 * time.Second):
						println(counter.String())
					case err := <-errors:
						println("FAILED", err)
						return nil
					}
				}
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		println(err)
	}
}
