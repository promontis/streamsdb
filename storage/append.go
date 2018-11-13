package storage

import (
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/c2h5oh/datasize"
	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

func (this *FdbStreams) Append(id StreamId, messages ...Message) (StreamPosition, error) {
	defer func(t time.Time) {
		this.metrics.storeAppendTime.UpdateSince(t)
		this.metrics.storeAppendCount.Count()
	}(time.Now())

	// prepare
	blockId, pointers, err := this.writeBlock(id, messages)
	if err != nil {
		err = errors.Wrap(err, "append failed")
		return NilStreamPosition, err
	}
	this.log.Debug("block prepared", zap.String("block-id", blockId.String()))

	// commit
	streams := []StreamId{
		StreamId(fmt.Sprintf("$global.%v", xxhash.Sum64String(id.String())%1024)),
		id,
	}

	pos, err := this.linkBlock(streams, blockId, pointers)
	if err != nil {
		err = errors.Wrap(err, "append failed")

		return NilStreamPosition, err
	}

	this.log.Debug("append success", zap.Any("commit", pos))
	return pos.Positions[id], nil
}

func (this *FdbStreams) writeBlock(id StreamId, messages []Message) (xid.ID, []MessagePointer, error) {
	defer func(t time.Time) {
		this.metrics.blockWriteTime.UpdateSince(t)
		this.metrics.blockWriteCount.Count()
	}(time.Now())

	blockId := xid.New()

	block := this.rootSpace.Block(blockId)
	if err := block.Set(messages); err != nil {
		return blockId, nil, err
	}

	pointers := make([]MessagePointer, len(messages))
	for i, m := range messages {
		pointers[i] = MessagePointer{
			BlockId:      blockId,
			MessageIndex: i,
			HeaderSize:   len(m.Header),
			ValueSize:    len(m.Payload),
		}
	}

	return blockId, pointers, nil
}

type CommitResult struct {
	Positions map[StreamId]StreamPosition
	BlockId   xid.ID
}

func (this *FdbStreams) linkBlock(streamIds []StreamId, blockId xid.ID, pointers []MessagePointer) (CommitResult, error) {
	defer func(t time.Time) {
		this.metrics.blockConnectTime.UpdateSince(t)
		this.metrics.blockConnectCount.Count()
		this.metrics.blockConnectStreamCount.Inc(int64(len(streamIds)))
	}(time.Now())

	type Result struct {
		Stream   StreamId
		Position StreamPosition
		Error    error
	}
	// TODO: remove unsuccesful blocks
	pos, err := this.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		positions := make(map[StreamId]StreamPosition, len(streamIds))
		results := make(chan Result)

		for _, streamId := range streamIds {
			go func(streamId StreamId) {
				stream := this.rootSpace.Stream(streamId)
				start, err := stream.AppendBlockMessages(tx, blockId, pointers)

				results <- Result{streamId, start, err}
			}(streamId)

		}

		var err error
		for range streamIds {
			result := <-results
			if result.Error != nil {
				err = result.Error
			} else {
				positions[result.Stream] = result.Position
			}
		}

		return CommitResult{
			BlockId:   blockId,
			Positions: positions,
		}, err
	})

	if err != nil {
		return CommitResult{}, errors.Wrap(err, "link block error")
	}

	commit, ok := pos.(CommitResult)
	if !ok {
		panic("invalid transaction result type")
	}
	this.log.Debug("block linked", zap.Any("commit", commit))

	return commit, err
}

type Chunk struct {
	Index    int
	Position int
	Size     datasize.ByteSize
	Messages []Message
}

func (this Chunk) Len() int {
	return len(this.Messages)
}
