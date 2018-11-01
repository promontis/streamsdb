package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/c2h5oh/datasize"
	"github.com/davecgh/go-xdr/xdr2"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	ElemBlocks   = iota
	ElemBlock    = iota
	ElemHeader   = iota
	ElemValue    = iota
	ElemPosition = iota
)

type RootSpace struct {
	subspace.Subspace
}

func (this RootSpace) Stream(id StreamId) StreamSpace {
	return StreamSpace{this.Sub(id)}
}

func (this StreamSpace) SetPosition(tx fdb.Transaction, pos StreamPosition) error {
	tx.Set(this.Position(), pos.Write())
	return nil
}

type StreamSpace struct{ subspace.Subspace }

type StreamVersion int64

func (this StreamVersion) Next() StreamVersion {
	return this + 1
}

func (this StreamSpace) BlockRelations() BlockRelationsSpace {
	return BlockRelationsSpace{this.Sub(ElemBlocks)}
}

type BlockRelationsSpace struct {
	subspace.Subspace
}

type BlockRelationSpace struct {
	subspace.Subspace
}

func (this StreamSpace) WriteBlockRelation(tx fdb.Transaction, pos StreamPosition, blockId uuid.UUID) error {
	tx.Set(this.Sub(ElemBlocks, int64(pos)), blockId[:])
	return nil
}

func (this StreamSpace) PositionToBlockIndex() PositionsToBlockIndexSpace {
	return PositionsToBlockIndexSpace{this.Sub("pos-to-block-elements")}
}

type PositionsToBlockIndexSpace struct {
	subspace.Subspace
}

func (this PositionsToBlockIndexSpace) Position(pos StreamPosition) PositionToBlockIndexSpace {
	return PositionToBlockIndexSpace{this.Sub(pos)}
}

type PositionToBlockIndexSpace struct {
	subspace.Subspace
}

func (this PositionToBlockIndexSpace) Set(tx fdb.Transaction, value BlockMessagePointer) error {
	var buffer bytes.Buffer
	if _, err := xdr.Marshal(&buffer, value); err != nil {
		return errors.Wrap(err, "marshal error")
	}

	tx.Set(this, buffer.Bytes())
	return nil
}

func (this StreamSpace) ReadPosition(tx fdb.ReadTransaction) (StreamPosition, error) {
	value, err := tx.Get(this.Position()).Get()
	if err != nil {
		return NilStreamPosition, err
	}
	if value == nil {
		return NilStreamPosition, nil
	}
	return NewStreamPosition(value), nil
}

type MessageSpace struct{ subspace.Subspace }

func (this MessageSpace) Header() ValueSpace {
	return ValueSpace{this.Sub(ElemHeader)}
}

func (this MessageSpace) Value() ValueSpace {
	return ValueSpace{this.Sub(ElemValue)}
}

const (
	SingleValue = byte(iota)
	MultiValue  = byte(iota)

	// the maximum size of a value stores in a value space
	ValuePartLimit = int(10 * datasize.KB)
)

type ValueSpace struct{ subspace.Subspace }

func (this ValueSpace) Set(tx fdb.Transaction, value []byte) {
	// write single value
	if len(value) <= ValuePartLimit {
		buf := make([]byte, len(value)+1)
		buf[0] = SingleValue
		copy(buf[1:], value)

		tx.Set(this.Sub(0), buf)
		return
	}

	// write multi value
	i := 1
	buf := value[ValuePartLimit:]

	// first write all values, except the first one
	for len(buf) > 0 {
		take := ValuePartLimit
		if take > len(buf) {
			take = len(buf)
		}

		tx.Set(this.Sub(i), buf[0:take])
		buf = buf[take:]
		i++
	}

	// now we know how much values we have written, so
	// write the first value including the meta data
	first := make([]byte, 7+ValuePartLimit)
	copy(first[7:], value)
	first[0] = MultiValue
	binary.BigEndian.PutUint16(first[1:], uint16(i))
	binary.BigEndian.PutUint32(first[3:], uint32(len(value)))

	tx.Set(this.Sub(0), first)
}

// Read read a value from the value space. It handles single and
// multipart values and is optimized to want the full value at once.
func (this ValueSpace) Read(tx fdb.ReadTransaction) ([]byte, error) {
	if firstValue := tx.Get(this.Sub(0)).MustGet(); len(firstValue) > 0 {
		valueType := firstValue[0]

		if valueType == SingleValue {
			return firstValue[1:], nil
		}
		if valueType != MultiValue {
			return nil, errors.New("unexpected first value byte")
		}

		n := int(binary.BigEndian.Uint16(firstValue[1:]))
		s := int(binary.BigEndian.Uint32(firstValue[3:]))

		value := append(make([]byte, 0, s), firstValue[7:]...)

		keyRange := fdb.KeyRange{this.Sub(1), this.Sub(n + 1)}
		result := tx.GetRange(keyRange, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll})
		kvs, err := result.GetSliceWithError()

		if err != nil {
			return nil, errors.Wrap(err, "get range failed")
		}

		for _, kv := range kvs {
			value = append(value, kv.Value...)
		}
		if s != len(value) {
			return nil, errors.New(fmt.Sprintf("read short: %v instead of %v", len(value), s))
		}

		return value, nil
	}
	return nil, errors.New("not found")
}

func (this ValueSpace) LengthKey() fdb.Key {
	return this.Sub(1).FDBKey()
}
