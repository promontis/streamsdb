package storage

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/c2h5oh/datasize"
	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

func (this *FdbStreams) Append(id StreamId, messages ...Message) (StreamPosition, error) {
	// create chunks
	chunks, err := toChunks(messages)
	if err != nil {
		err = errors.Wrap(err, "chunks creation failed")
		return NilStreamPosition, err
	}
	this.log.Debug("append started", zap.Stringer("stream", id), zap.Int("message-count", len(messages)), zap.Int("chunk-count", len(chunks)))

	// prepare
	blockId, err := this.writeBlock(id, chunks)
	if err != nil {
		err = errors.Wrap(err, "append failed")
		return NilStreamPosition, err
	}
	this.log.Debug("block prepared", zap.String("block-id", blockId.String()))

	// commit
	pos, err := this.safelyLinkBlock(id, blockId, messages)
	if err != nil {
		err = errors.Wrap(err, "append failed")

		return NilStreamPosition, err
	}

	this.log.Debug("append success", zap.Any("commit", pos))
	return pos.StreamPosition, nil
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

func (this *FdbStreams) writeBlock(id StreamId, chunks []Chunk) (xid.ID, error) {
	done := make(chan struct{})
	defer close(done)

	blockId := xid.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	complete := make(chan error)
	block := this.rootSpace.Block(blockId)

	for _, c := range chunks {
		go this.writeChunk(ctx, block, c.Position, c.Messages, complete)
	}

	for range chunks {
		if err := <-complete; err != nil {
			// TODO: cleanup chunks?
			return blockId, err
		}
	}
	return blockId, nil
}

type CommitResult struct {
	StreamId       StreamId
	BlockId        xid.ID
	Partition      int64
	StreamPosition StreamPosition
	TailPosition   StreamPosition
}

func (this *FdbStreams) safelyLinkBlock(streamId StreamId, blockId xid.ID, messages []Message) (CommitResult, error) {
	// TODO: remove unsuccesful blocks
	pos, err := this.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		stream := this.rootSpace.Stream(streamId)
		pos, err := stream.ReadPosition(tx)
		if err != nil {
			return CommitResult{}, errors.Wrap(err, "read stream position failed")
		}

		start := pos.Next()
		index := stream.PositionToBlockIndex()
		for i, m := range messages {
			head := start.NextN(i)
			pointer := BlockMessagePointer{
				BlockId:      blockId,
				MessageIndex: i,
				HeaderSize:   len(m.Header),
				ValueSize:    len(m.Payload),
			}

			this.log.Debug("pointer set", zap.Stringer("position", head), zap.Any("pointer", pointer))

			index.Position(head).Set(tx, pointer)
		}

		stream.SetPosition(tx, start.NextN(len(messages)-1))
		partition := int64(xxhash.Sum64([]byte(streamId))) % 255
		tailPos, err := this.rootSpace.Tail().Partition(partition).AppendBlock(tx, streamId, blockId)
		if err != nil {
			return CommitResult{}, errors.Wrap(err, "tail append error")
		}
		return CommitResult{
			StreamId:       streamId,
			BlockId:        blockId,
			StreamPosition: start,
			Partition:      partition,
			TailPosition:   tailPos,
		}, nil
	})

	return pos.(CommitResult), err
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
