package storage

import (
	"io/ioutil"
	"log"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	goes "github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/pjvds/randombytes"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

func BenchmarkDiffDbsStreamAppend(b *testing.B) {
	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	store := OpenFdb(db, zap.NewNop())
	value := randombytes.Make(100)
	stream := StreamId(xid.New().String())
	message := Message{
		Payload: value,
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := store.Append(stream, message)
		if err != nil {
			b.Fatalf("append error: %v", err)
		}
	}
}

func BenchmarkDiffDbsFdbSetKey(b *testing.B) {
	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()

	value := randombytes.Make(100)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		tx, err := db.CreateTransaction()
		if err != nil {
			b.Fatalf("transaction failed: %v", err.Error())
		}

		tx.Set(tuple.Tuple{"b", n}, value)
		tx.Commit().MustGet()

		if err != nil {
			b.Fatalf("transaction failed: %v", err.Error())
		}
	}
}

func BenchmarkDiffDbsGetEventStoreAppend(b *testing.B) {
	log.SetOutput(ioutil.Discard)
	//connect to a single node
	config := &goes.Configuration{
		ReconnectionDelay:   10000,
		MaxReconnects:       10,
		MaxOperationRetries: 10,
		Address:             "127.0.0.1",
		Port:                1113,
		Login:               "admin",
		Password:            "changeit",
	}

	conn, err := goes.NewEventStoreConnection(config)
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	err = conn.Connect()
	defer conn.Close()
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	value := randombytes.Make(100)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		events := []goes.Event{
			{
				EventID:   [16]byte(uuid.New()),
				EventType: "f",
				IsJSON:    false,
				Data:      value,
			},
		}
		result, err := goes.AppendToStream(conn, "shoppingCart-1", -2, events)
		if *result.Result != protobuf.OperationResult_Success {
			log.Printf("[info] WriteEvents failed. %v", result.Result.String())
		}
		if err != nil {
			log.Printf("[error] WriteEvents failed. %v", err.Error())
		}
	}
}
