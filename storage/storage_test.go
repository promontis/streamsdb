package storage

import (
	"math/rand"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pjvds/randombytes"
	"github.com/stretchr/testify/assert"
)

func TestChunk(t *testing.T) {
	messages := []Message{
		{Payload: randombytes.Make(int(CHUNK_LIMIT / 3))}, // chunk 1
		{Payload: randombytes.Make(int(CHUNK_LIMIT / 3))}, // chunk 1
		{Payload: randombytes.Make(int(CHUNK_LIMIT / 3))}, // chunk 1
		{Payload: randombytes.Make(int(CHUNK_LIMIT / 3))}, // chunk 2
		{Payload: randombytes.Make(int(CHUNK_LIMIT / 3))}, // chunk 2
		{Payload: randombytes.Make(int(CHUNK_LIMIT / 3))}, // chunk 2
		{Payload: randombytes.Make(int(CHUNK_LIMIT / 3))}, // chunk 3
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
