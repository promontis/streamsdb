package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/c2h5oh/datasize"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	MESSAGE_HEADER_LIMIT  = 1 * datasize.MB
	MESSAGE_PAYLOAD_LIMIT = 5 * datasize.MB
	CHUNK_LIMIT           = 9 * datasize.B
	VALUE_LIMIT           = 10 * datasize.KB
)

type Message struct {
	Header  []byte
	Payload []byte
}

type StreamId string

func (this StreamId) Stringer() string {
	return string(this)
}

func (this StreamId) FDBKey() fdb.Key {
	return []byte(this)
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

func OpenFdb(db fdb.Database, log *zap.Logger) FdbStreams {
	return FdbStreams{
		db:        db,
		log:       log,
		rootSpace: RootSpace{subspace.Sub("s")},
	}
}

type FdbStreams struct {
	db        fdb.Database
	rootSpace RootSpace
	log       *zap.Logger
}

var bin = binary.BigEndian

func PutUint64(value uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}

// readValue should not be blocking!
func (this *FdbStreams) readValue(tx fdb.ReadTransaction, space ValueSpace) ([]byte, error) {
	if value := tx.Get(space.Sub("ns")).MustGet(); value != nil {
		n := int(binary.BigEndian.Uint16(value[0:]))
		s := int(binary.BigEndian.Uint32(value[2:]))

		if n == 0 {
			return []byte{}, nil
		}

		value := make([]byte, 0, s)
		chunks := make([]fdb.FutureByteSlice, n, n)
		for chunk := 0; chunk < n; chunk++ {
			chunks[chunk] = tx.Get(space.Sub(chunk))
		}

		for chunk := 0; chunk < n; chunk++ {
			value = append(value, chunks[chunk].MustGet()...)
		}

		if s != len(value) {
			return nil, errors.New("read short")
		}

		return value, nil
	}
	return nil, errors.New("not found")
}

func (this *FdbStreams) writeValue(tx fdb.Transaction, space ValueSpace, value []byte) error {
	toWrite := len(value)

	chunks := 1
	if toWrite > VALUE_LIMIT {
		chunks = int(math.Ceil(float64(toWrite) / VALUE_LIMIT))
	}

	ns := make([]byte, 6, 6)
	binary.BigEndian.PutUint16(ns[0:], uint16(chunks))
	binary.BigEndian.PutUint32(ns[2:], uint32(toWrite))

	tx.Set(space.Sub("ns"), ns)

	// write all chunks except last
	for chunk := 0; chunk < int(chunks); chunk++ {
		min := func(a, b int) int {
			if a < b {
				return a
			}
			return b
		}

		start := chunk * limit
		length := start + min(toWrite-start, limit)

		sub := value[start:length]
		tx.Set(space.Sub(chunk), sub)
	}
	return nil
}

func (this StreamPosition) IsBeforeOrAt(pos StreamPosition) bool {
	return this <= pos
}

type BlockSpace struct {
	subspace.Subspace
}

func (this BlockSpace) Message(n int) MessageSpace {
	return MessageSpace{this.Sub(n)}
}

func (this RootSpace) Block(id uuid.UUID) BlockSpace {
	return BlockSpace{this.Sub(id[:])}
}

func (this *FdbStreams) writeChunk(ctx context.Context, blockId uuid.UUID, c Chunk, complete chan error) {
	_, err := this.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		block := this.rootSpace.Block(blockId)

		for i, m := range c.Messages {
			if err := this.writeValue(tx, block.Message(i).Header(), m.Header); err != nil {
				return nil, err
			}
			if err := this.writeValue(tx, block.Message(i).Value(), m.Payload); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})

	select {
	case complete <- err:
	case <-ctx.Done():
	}
}
