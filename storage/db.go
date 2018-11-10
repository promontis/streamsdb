package storage

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	xdr "github.com/davecgh/go-xdr/xdr2"
	"github.com/pkg/errors"
	"github.com/rs/xid"
)

type TailSpace struct {
	subspace.Subspace
}

type PartitionSpace struct {
	subspace.Subspace
}

type TailEntry struct {
	Stream  StreamId
	BlockId xid.ID
}

func (this TailEntry) MarshalBinary() ([]byte, error) {
	var buffer bytes.Buffer
	_, err := xdr.Marshal(&buffer, this)
	return buffer.Bytes(), err
}

func (this *TailEntry) UnmarshalBinary(data []byte) error {
	_, err := xdr.Unmarshal(bytes.NewReader(data), this)
	return err
}

func (this PartitionSpace) AppendBlock(tx fdb.Transaction, stream StreamId, blockId xid.ID) (StreamPosition, error) {
	pos, err := this.ReadHead(tx)
	if err != nil {
		return NilStreamPosition, err
	}

	entry := TailEntry{
		Stream:  stream,
		BlockId: blockId,
	}

	value, err := entry.MarshalBinary()
	if err != nil {
		return pos, errors.Wrap(err, "tail entry marshal error")
	}

	tx.Set(this.Sub(pos), value)
	this.SetHead(tx, pos.Next())
	return pos.Next(), nil
}

func (this PartitionSpace) SetHead(tx fdb.Transaction, position StreamPosition) {
	tx.Set(this.Sub("h"), position.Write())
}

func (this PartitionSpace) ReadHead(tx fdb.ReadTransaction) (StreamPosition, error) {
	value, err := tx.Get(this.Sub("h")).Get()
	if err != nil {
		return NilStreamPosition, err
	}
	if value == nil {
		return NilStreamPosition, nil
	}
	return NewStreamPosition(value), nil
}

func (this TailSpace) Partition(p int64) PartitionSpace {
	return PartitionSpace{this.Sub(p)}
}

func (this PartitionSpace) CreateWatch(db fdb.Database) (fdb.FutureNil, error) {
	w, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		tx.Get(this.Sub("h"))
		return tx.Watch(this.Sub("h")), nil
	})
	if err != nil {
		return nil, err
	}

	return w.(fdb.FutureNil), nil
}
