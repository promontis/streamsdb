package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"time"

	stdlog "log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/c2h5oh/datasize"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

const (
	MESSAGE_HEADER_LIMIT  = 1 * datasize.MB
	MESSAGE_PAYLOAD_LIMIT = 5 * datasize.MB
	CHUNK_LIMIT           = 9 * datasize.MB
	VALUE_LIMIT           = 10 * datasize.KB
)

type Message struct {
	Header  []byte
	Payload []byte
}

type StreamId string

func (this StreamId) String() string {
	return string(this)
}

func (this StreamId) FDBKey() fdb.Key {
	return tuple.Tuple{string(this)}.Pack()
}

type StreamPosition int64

func (this StreamPosition) FDBKey() fdb.Key {
	return tuple.Tuple{int64(this)}.Pack()
}

func (this StreamPosition) String() string {
	return strconv.FormatInt(int64(this), 10)
}

func (this StreamPosition) OrLower(pos StreamPosition) StreamPosition {
	if this < pos {
		return this
	}
	return pos
}

func (this StreamPosition) OrHigher(pos StreamPosition) StreamPosition {
	if this > pos {
		return this
	}
	return pos
}

const NilStreamPosition = StreamPosition(0)

func (this StreamPosition) BeforeOrEqual(n StreamPosition) bool {
	return this <= n
}
func (this StreamPosition) IsAfter(n StreamPosition) bool {
	return this > n
}

func (this StreamPosition) NextN(n int) StreamPosition {
	return StreamPosition(int64(this) + int64(n))
}

func (this StreamPosition) PreviousN(n int) StreamPosition {
	return StreamPosition(int64(this) - int64(n))
}

func NewStreamPosition(buf []byte) StreamPosition {
	return StreamPosition(binary.BigEndian.Uint64(buf))
}

func (this StreamPosition) Next() StreamPosition {
	return this + 1
}

func (this StreamPosition) Stringer() string {
	return fmt.Sprintf("p%v", this)
}

func (this StreamPosition) Write() []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(this))
	return buf
}

func (this StreamSpace) Position() subspace.Subspace {
	return this.Sub(ElemPosition)
}

func OpenFdb(db fdb.Database, log *zap.Logger) *FdbStreams {
	this := &FdbStreams{
		db:  db,
		log: log,
		metrics: metricsContext{
			storeAppendTime:         metrics.GetOrRegisterTimer("store.append.time", metrics.DefaultRegistry),
			storeAppendCount:        metrics.GetOrRegisterCounter("store.append.count", metrics.DefaultRegistry),
			blockWriteTime:          metrics.GetOrRegisterTimer("block.write.time", metrics.DefaultRegistry),
			blockWriteCount:         metrics.GetOrRegisterCounter("block.write.count", metrics.DefaultRegistry),
			blockConnectTime:        metrics.GetOrRegisterTimer("block.connect.time", metrics.DefaultRegistry),
			blockConnectCount:       metrics.GetOrRegisterCounter("block.connect.count", metrics.DefaultRegistry),
			blockConnectStreamCount: metrics.GetOrRegisterCounter("block.connect.streams.Count", metrics.DefaultRegistry),
		},
	}
	go metrics.LogScaledWithContext(context.Background(), metrics.DefaultRegistry, 5*time.Second, time.Millisecond, stdlog.New(os.Stderr, "metrics: ", stdlog.Lmicroseconds))

	this.rootSpace = RootSpace{this, subspace.Sub("s")}
	return this
}

type metricsContext struct {
	storeAppendTime         metrics.Timer
	storeAppendCount        metrics.Counter
	blockWriteTime          metrics.Timer
	blockWriteCount         metrics.Counter
	blockConnectTime        metrics.Timer
	blockConnectCount       metrics.Counter
	blockConnectStreamCount metrics.Counter
}

type FdbStreams struct {
	db        fdb.Database
	rootSpace RootSpace
	log       *zap.Logger
	metrics   metricsContext
}

func PutUint64(value uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}

func (this StreamPosition) IsBeforeOrAt(pos StreamPosition) bool {
	return this <= pos
}

type BlockSpace struct {
	*FdbStreams
	subspace.Subspace
}

func (this BlockSpace) Message(n int) MessageSpace {
	return MessageSpace{this.Sub(n)}
}

func (this RootSpace) Block(id xid.ID) BlockSpace {
	return BlockSpace{this.FdbStreams, this.Sub(id[:])}
}

func createWriteJobsForMessages(messages []Message, limit datasize.ByteSize) []Chunk {
	left := messages
	position := 0
	chunked := make([]Chunk, 0, 1)

	for len(left) != 0 {
		take := 0
		size := 0 * datasize.B
		for _, m := range left {
			growth := datasize.ByteSize((len(m.Header) + len(m.Payload)))
			if size+growth > CHUNK_LIMIT {
				panic("message size larger than chunk limit")
			}

			take++
			size += growth
		}

		chunked = append(chunked, Chunk{
			Index:    len(chunked),
			Position: position,
			Messages: left[0:take],
			Size:     size,
		})

		left = left[take:]
		position += take
	}
	return chunked
}

func (this BlockSpace) Set(messages []Message) error {
	chunked := createWriteJobsForMessages(messages, CHUNK_LIMIT)

	if len(chunked) == 1 {
		// fast path for single chunk write
		c := chunked[0]
		return this.writeChunk(c.Position, c.Messages)
	}

	complete := make(chan error)
	for _, c := range chunked {
		go func(c Chunk) {
			complete <- this.writeChunk(c.Position, c.Messages)
		}(c)
	}

	for range chunked {
		err := <-complete
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *BlockSpace) writeChunk(offset int, messages []Message) error {
	_, err := this.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		for i, m := range messages {
			message := this.Message(offset + i)
			message.Header().Set(tx, m.Header)
			message.Value().Set(tx, m.Payload)
		}
		return nil, nil
	})
	return err
}
