package storage

import (
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/google/uuid"
)

const (
	ElemBlocks   = "blocks"
	ElemBlock    = "block"
	ElemHeader   = "header"
	ElemValue    = "value"
	ElemPosition = "position"
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

func (this PositionToBlockIndexSpace) Set(tx fdb.Transaction, blockId uuid.UUID, messageIndex int, messageSize int) error {
	indexBuf := make([]byte, 16)
	binary.BigEndian.PutUint32(indexBuf, uint32(messageIndex))
	binary.BigEndian.PutUint32(indexBuf[4:], uint32(messageSize))

	tx.Set(this, append(blockId[:], indexBuf...))
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

type ValueSpace struct{ subspace.Subspace }

func (this ValueSpace) LengthKey() fdb.Key {
	return this.Sub(1).FDBKey()
}
