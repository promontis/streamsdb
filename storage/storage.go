package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/c2h5oh/datasize"
	"github.com/pkg/errors"
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

func (this StreamPosition) IsBeforeOrAt(pos StreamPosition) bool {
	return this <= pos
}

type BlockSpace struct {
	subspace.Subspace
}

func (this BlockSpace) Message(n int) MessageSpace {
	return MessageSpace{this.Sub(n)}
}

func (this RootSpace) Block(id xid.ID) BlockSpace {
	return BlockSpace{this.Sub(id[:])}
}

func (this *FdbStreams) writeChunk(ctx context.Context, block BlockSpace, offset int, messages []Message, complete chan error) {
	_, err := this.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		for i, m := range messages {
			message := block.Message(offset + i)
			message.Header().Set(tx, m.Header)
			message.Value().Set(tx, m.Payload)
		}
		return nil, nil
	})

	select {
	case complete <- err:
	case <-ctx.Done():
	}
}
