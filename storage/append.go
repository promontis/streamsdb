package storage

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/c2h5oh/datasize"
	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

func (this *FdbStreams) Append(id StreamId, messages ...Message) (StreamPosition, error) {
	// prepare
	blockId, pointers, err := this.writeBlock(id, messages)
	if err != nil {
		err = errors.Wrap(err, "append failed")
		return NilStreamPosition, err
	}
	this.log.Debug("block prepared", zap.String("block-id", blockId.String()))

	globalPartitionStream := StreamId(fmt.Sprintf("$global.%v", xxhash.Sum64String(id.String())%255))

	// commit
	streams := []StreamId{
		globalPartitionStream,
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

func toChunks(messages []Message) ([]Chunk, error) {
	left := messages
	position := 0
	chunked := make([]Chunk, 0)

	for len(left) != 0 {
		take := 0
		size := 0 * datasize.B
		for _, m := range left {
			growth := datasize.ByteSize((len(m.Header) + len(m.Payload)))
			if size+growth > CHUNK_LIMIT {
				if take == 0 {
					panic("take should never be 0")
				}

				break
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
	return chunked, nil
}

func (this *FdbStreams) writeBlock(id StreamId, messages []Message) (xid.ID, []BlockMessagePointer, error) {
	blockId := xid.New()

	// create chunks
	chunks, err := toChunks(messages)
	if err != nil {
		err = errors.Wrap(err, "chunks creation failed")
		return blockId, nil, err
	}

	done := make(chan struct{})
	defer close(done)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	complete := make(chan error)
	block := this.rootSpace.Block(blockId)

	for _, c := range chunks {
		go this.writeChunk(ctx, block, c.Position, c.Messages, complete)
	}

	pointers := make([]BlockMessagePointer, len(messages))
	for i, m := range messages {
		pointers[i] = BlockMessagePointer{
			BlockId:      blockId,
			MessageIndex: i,
			HeaderSize:   len(m.Header),
			ValueSize:    len(m.Payload),
		}
	}

	for range chunks {
		if err := <-complete; err != nil {
			// TODO: cleanup chunks?
			return blockId, pointers, err
		}
	}
	return blockId, pointers, nil
}

type CommitResult struct {
	Positions map[StreamId]StreamPosition
	BlockId   xid.ID
}

func (this *FdbStreams) linkBlock(streamIds []StreamId, blockId xid.ID, pointers []BlockMessagePointer) (CommitResult, error) {
	// TODO: remove unsuccesful blocks
	pos, err := this.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		positions := make(map[StreamId]StreamPosition, len(streamIds))

		for _, streamId := range streamIds {
			stream := this.rootSpace.Stream(streamId)
			start, err := stream.AppendBlockMessages(tx, blockId, pointers)
			if err != nil {
				return CommitResult{}, errors.Wrap(err, "append block to stream failure")
			}

			positions[streamId] = start
		}

		return CommitResult{
			BlockId:   blockId,
			Positions: positions,
		}, nil
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
