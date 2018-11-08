package storage

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/davecgh/go-xdr/xdr2"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/xid"
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

func (stream StreamSpace) AppendBlockMessages(tx fdb.Transaction, blockId xid.ID, pointers []BlockMessagePointer) (StreamPosition, error) {
	pos, err := stream.ReadPosition(tx)
	if err != nil {
		return NilStreamPosition, errors.Wrap(err, "read stream position failed")
	}

	start := pos.Next()
	index := stream.PositionToBlockIndex()
	for i, ptr := range pointers {
		head := start.NextN(i)
		index.Position(head).Set(tx, ptr)
	}

	stream.SetPosition(tx, start.NextN(len(pointers)-1))
	return start, nil
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
