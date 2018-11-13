package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/c2h5oh/datasize"
	"github.com/davecgh/go-xdr/xdr2"
	"github.com/pjvds/randombytes"
	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestBlockMessagePointerMarshalling(t *testing.T) {
	pointer := MessagePointer{
		BlockId:      xid.New(),
		MessageIndex: 42,
		HeaderSize:   43,
		ValueSize:    44,
	}

	var buffer bytes.Buffer
	_, err := xdr.Marshal(&buffer, pointer)
	if err != nil {
		t.Fatalf("marshal failed: %v", err.Error())
		return
	}

	var unmarshalled MessagePointer
	if _, err := xdr.Unmarshal(&buffer, &unmarshalled); err != nil {
		t.Fatalf("unmarshal failed: %v", err.Error())
		return
	}

	assert.Equal(t, unmarshalled, pointer)
}

func TestSimpleStorage(t *testing.T) {
	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	store := OpenFdb(db, zap.NewNop())
	rand.Seed(time.Now().UnixNano())

	stream := StreamId(fmt.Sprintf("stream-%v", rand.Uint64()))

	t.Run("single message roundtrip", func(t *testing.T) {
		header := []byte("header")
		value := []byte("value")

		pos, err := store.Append(stream, Message{header, value})
		if err != nil {
			t.Fatalf("append error: %v", err)
		}

		result, err := store.Read(stream, pos, 1)
		if err != nil {
			t.Fatalf("read error: %v", err)
		}

		if !bytes.Equal(header, result.Messages[0].Header) {
			t.Errorf("header mismatch")
		}
		if !bytes.Equal(value, result.Messages[0].Payload) {
			t.Errorf("value mismatch")
		}
	})
	t.Run("multiple appends", func(t *testing.T) {
		appends := 3
		firstPos, err := store.Append(stream, Message{Payload: []byte("0")})
		if err != nil {
			t.Fatalf("append error: %v", err)
		}

		for i := 1; i < appends; i++ {
			pos, err := store.Append(stream, Message{Payload: []byte(fmt.Sprint(i))})
			if err != nil {
				t.Fatalf("append error: %v", err)
			}

			assert.Equal(t, firstPos.NextN(i), pos)
		}

		result, err := store.Read(stream, firstPos, 10)

		assert.NoError(t, err)
		assert.Equal(t, firstPos, result.From)
		assert.Equal(t, appends, len(result.Messages))
		assert.Equal(t, firstPos.NextN(appends), result.Next)
		assert.Equal(t, "0", string(result.Messages[0].Payload))
		assert.Equal(t, "1", string(result.Messages[1].Payload))
	})
}

func BenchmarkStreamAppendAndReadRoundtrip(b *testing.B) {
	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	store := OpenFdb(db, zap.NewNop())
	rand.Seed(time.Now().UnixNano())
	roundtrip := func(b *testing.B, size datasize.ByteSize) {
		stream := StreamId(xid.New().String())
		message := Message{
			Payload: randombytes.Make(int(8 * datasize.MB)),
		}
		pos, _ := store.Append(stream, message)
		store.Read(stream, pos, 1)

		b.SetBytes(int64(size) * 2)
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			pos, err := store.Append(stream, message)
			if err != nil {
				b.Fatalf("append error: %v", err)
			}

			_, err = store.Read(stream, pos, 1)
			if err != nil {
				b.Fatalf("read error: %v", err)
			}
		}
	}

	b.Run("1kb", func(b *testing.B) { roundtrip(b, 1*datasize.KB) })
	b.Run("5kb", func(b *testing.B) { roundtrip(b, 5*datasize.KB) })
	b.Run("75kb", func(b *testing.B) { roundtrip(b, 75*datasize.KB) })
	b.Run("500kb", func(b *testing.B) { roundtrip(b, 500*datasize.KB) })
	b.Run("1mb", func(b *testing.B) { roundtrip(b, 1*datasize.MB) })
	b.Run("5mb", func(b *testing.B) { roundtrip(b, 1*datasize.MB) })
}

func BenchmarkStreamRead(b *testing.B) {
	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	store := OpenFdb(db, zap.NewNop())
	rand.Seed(time.Now().UnixNano())
	roundtrip := func(b *testing.B, size datasize.ByteSize) {
		stream := StreamId(xid.New().String())
		message := Message{
			Payload: randombytes.Make(int(8 * datasize.MB)),
		}
		pos, err := store.Append(stream, message)
		if err != nil {
			b.Fatalf("append error: %v", err)
		}

		b.SetBytes(int64(size) * 2)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			_, err := store.Read(stream, pos, 1)
			if err != nil {
				b.Fatalf("read error: %v", err)
			}
		}
	}

	b.Run("1kb", func(b *testing.B) { roundtrip(b, 1*datasize.KB) })
	b.Run("5kb", func(b *testing.B) { roundtrip(b, 5*datasize.KB) })
	b.Run("75kb", func(b *testing.B) { roundtrip(b, 75*datasize.KB) })
	b.Run("500kb", func(b *testing.B) { roundtrip(b, 500*datasize.KB) })
	b.Run("1mb", func(b *testing.B) { roundtrip(b, 1*datasize.MB) })
	b.Run("5mb", func(b *testing.B) { roundtrip(b, 1*datasize.MB) })
}

func TestStreamAppend(t *testing.T) {
	testMessage := func(i int) Message {
		return Message{
			Header:  []byte(fmt.Sprintf("header-%v", i)),
			Payload: []byte(fmt.Sprintf("payload-%v", i)),
		}
	}

	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	store := OpenFdb(db, zap.NewNop())
	rand.Seed(time.Now().UnixNano())

	stream := StreamId("test-" + t.Name())
	db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		tx.ClearRange(store.rootSpace.Stream(stream))
		return nil, nil
	})

	// The position before appending, NilPosition for new streams,
	// otherwise equal to number of messages in the stream.
	// Expect: [0]
	posBeforeAppend, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		return store.rootSpace.Stream(stream).ReadPosition(tx)
	})
	assert.Equal(t, NilStreamPosition, posBeforeAppend)
	assert.NoError(t, err)

	// Append 3 messages to the stream. Append position should be previous
	// position + 1, equal to the position of the first messages appended.
	// Expected: [1]
	messages := []Message{
		testMessage(1),
		testMessage(2),
		testMessage(3),
	}
	posAppended, err := store.Append(stream, messages...)
	assert.NoError(t, err)
	assert.Equal(t, StreamPosition(1), posAppended)

	// Position after append should be equal to the appending position plus message count.
	// Expected: [3]
	posAfterAppend, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		return store.rootSpace.Stream(stream).ReadPosition(tx)
	})
	assert.Equal(t, StreamPosition(len(messages)), posAfterAppend)

	// Validate the message by comparing the written messages by a read result.
	readResult, err := store.Read(stream, posAppended, len(messages))
	assert.NoError(t, err)
	assert.Equal(t, messages, readResult.Messages)
}

/*
func BenchmarkStreamAppendReadRoundtrip_Ranged(b *testing.B) {
	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	store := OpenFdb(db)
	rand.Seed(time.Now().UnixNano())

	stream := StreamId(rand.Uint64())
	payload := randombytes.Make(50e3)

	messages := make([]Message, 50, 50)
	for i := range messages {
		messages[i] = Message{
			Payload: payload,
		}
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		pos, err := store.Append(stream, messages)

		if err != nil {
			b.Fatalf("append failed: %v", err)
		}

		_, err = store.Read(stream, pos, len(messages))
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
}

func BenchmarkAppendStreamContention_w1(ctx *testing.B) {
	workers := 1
	streams := 1
	batchsize := 100
	payload := randombytes.Make(50)

	benchAppend(ctx, streams, workers, batchsize, payload)
}

func BenchmarkAppendStreamContention_w50(ctx *testing.B) {
	workers := 50
	streams := 1
	batchsize := 100
	payload := randombytes.Make(50)

	benchAppend(ctx, streams, workers, batchsize, payload)
}

func BenchmarkAppendStreamParallelism_w10(ctx *testing.B) {
	workers := 10
	streams := 10
	batchsize := 100
	payload := randombytes.Make(50)

	benchAppend(ctx, streams, workers, batchsize, payload)
}
func BenchmarkAppendStreamParallelism_w100(ctx *testing.B) {
	workers := 100
	streams := 100
	batchsize := 100
	payload := randombytes.Make(50)

	benchAppend(ctx, streams, workers, batchsize, payload)
}
func BenchmarkAppendStreamParallelism_w1000(ctx *testing.B) {
	workers := 100
	streams := 100
	batchsize := 100
	payload := randombytes.Make(50)

	benchAppend(ctx, streams, workers, batchsize, payload)
}
func BenchmarkAppendStreamParallelism_w100000(ctx *testing.B) {
	workers := 100
	streams := 100
	batchsize := 100
	payload := randombytes.Make(50)

	benchAppend(ctx, streams, workers, batchsize, payload)
}

/*
func benchAppend(ctx *testing.B, streams, workers, batchsize int, payload []byte) {
	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	store := OpenFdb(db, zap.NewNop())
	firehose := make(chan []byte)

	streamPool := make([]string, streams)
	for i := range streamPool {
		streamPool[i] = fmt.Sprintf("s%v", i)
	}

	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		stream := StreamId(streamPool[w%streams])
		wg.Add(1)

		go func() {
			defer wg.Done()

			batch := make([]Message, 0, batchsize)

			flush := func() error {
				_, err := store.Append(stream, batch...)
				batch = batch[:0]
				return err
			}

			write := func(value []byte) error {
				batch = append(batch, Message{
					Payload: value,
				})

				if len(batch) >= batchsize {
					return flush()
				}
				return nil
			}

			for value := range firehose {
				if err := write(value); err != nil {
					ctx.FailNow()
				}
			}
			flush()
		}()
	}
	ctx.SetBytes(int64(len(payload) * 10000))
	ctx.ResetTimer()
	for benchN := 0; benchN < ctx.N; benchN++ {
		for i := 0; i < 10000; i++ {
			firehose <- payload
		}
	}
	close(firehose)
	wg.Wait()

}*/
