package storage

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/c2h5oh/datasize"
	"github.com/pkg/errors"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

func (this *FdbStreams) Append(id StreamId, messages ...Message) (StreamPosition, error) {
	this.log.Debug("append started", zap.String("stream-id", id.Stringer()), zap.Int("message-count", len(messages)))

	// create chunks
	chunks, err := toChunks(messages)
	if err != nil {
		err = errors.Wrap(err, "chunks creation failed")
		return NilStreamPosition, err
	}

	// prepare
	blockId, err := this.writeBlock(id, chunks)
	if err != nil {
		err = errors.Wrap(err, "append failed")
		return NilStreamPosition, err
	}
	this.log.Debug("block prepared", zap.String("block-id", blockId.String()))

	// commit
	pos, err := this.safelyLinkBlock(this.rootSpace.Stream(id), blockId, messages)
	if err != nil {
		err = errors.Wrap(err, "append failed")

		return NilStreamPosition, err
	}
	this.log.Debug("block commited", zap.String("block-id", blockId.String()))
	return pos, nil
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

	for i := 0; i < len(chunks); i++ {
		if err := <-complete; err != nil {
			// TODO: cleanup chunks?
			return blockId, err
		}
	}
	return blockId, nil
}

func (this *FdbStreams) safelyLinkBlock(stream StreamSpace, blockId xid.ID, messages []Message) (StreamPosition, error) {
	// TODO: remove unsuccesful blocks
	pos, err := this.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		head, err := stream.ReadPosition(tx)
		if err != nil {
			return NilStreamPosition, errors.Wrap(err, "read stream position failed")
		}

		index := stream.PositionToBlockIndex()
		for i, m := range messages {
			head = head.Next()
			index.Position(head).Set(tx, blockId, i, len(m.Header)+len(m.Payload))
		}

		stream.SetPosition(tx, head)
		return head, nil
	})

	return pos.(StreamPosition), err
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
