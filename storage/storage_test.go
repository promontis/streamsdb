package storage

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/c2h5oh/datasize"
	"github.com/pjvds/randombytes"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

func TestStreamAppendRoundtripWithLargePayload(t *testing.T) {
	header := randombytes.Make(int(1 * datasize.MB))
	value := randombytes.Make(int(8 * datasize.MB))

	fdb.MustAPIVersion(520)
	db := fdb.MustOpenDefault()
	store := OpenFdb(db, zap.NewNop())
	rand.Seed(time.Now().UnixNano())

	stream := StreamId(rand.Uint64())

	pos, err := store.Append(stream, Message{header, value})
	if err != nil {
		t.Fatalf("append error: %v", err)
	}

	//	assert.Len(t, toChunks(messages[0:2]), 1)
	assert.Equal(t, len(toChunks(messages[:1])), 1)
	assert.Equal(t, len(toChunks(messages[:2])), 1)
	assert.Equal(t, len(toChunks(messages[:3])), 1)
	assert.Equal(t, len(toChunks(messages[:4])), 2)
	assert.Equal(t, len(toChunks(messages[:5])), 2)
	assert.Equal(t, len(toChunks(messages[:6])), 2)
	assert.Equal(t, len(toChunks(messages[:7])), 3)
}

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
