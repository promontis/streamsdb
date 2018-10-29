package main

import (
	"bufio"
	"os"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pjvds/streamdb/storage"
	"github.com/urfave/cli"
	"go.uber.org/zap"
)

func main() {
	log, err := zap.NewDevelopmentConfig().Build()
	if err != nil {
		println(err)
		return
	}

	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	streamdb := storage.OpenFdb(db, log)

	app := cli.NewApp()
	app.Name = "sdb-cli"
	app.Commands = []cli.Command{
		{
			Name: "append",
			Action: func(c *cli.Context) error {
				stream := storage.StreamId(c.Args().First())

				reader := bufio.NewReader(os.Stdin)
				value, err := reader.ReadString('\n')
				for err == nil {
					if pos, err := streamdb.Append(stream, storage.Message{Payload: []byte(value)}); err != nil {
						return err
					} else {
						println(pos)
						value, err = reader.ReadString('\n')
					}
				}
				return err
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
	}

	if err := app.Run(os.Args); err != nil {
		println(err)
	}
}
