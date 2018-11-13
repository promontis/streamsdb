package storage

import (
	"bytes"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/c2h5oh/datasize"
	"github.com/davecgh/go-xdr/xdr2"
	"github.com/pkg/errors"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

type ReadResult struct {
	// TODO: include HEAD
	Stream   StreamId
	From     StreamPosition
	Next     StreamPosition
	HasNext  bool
	Messages []Message
}

type MessagePointer struct {
	BlockId      xid.ID
	MessageIndex int
	HeaderSize   int
	ValueSize    int
}

func (this MessagePointer) String() string {
	return fmt.Sprintf("%v/%v", this.BlockId, this.MessageIndex)
}

type ReadPreparationState struct {
	From     StreamPosition
	Head     StreamPosition
	Pointers []MessagePointer
}

func (this *FdbStreams) pre(stream StreamSpace, from StreamPosition, length int) (ReadPreparationState, error) {
	result, err := this.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		head, err := stream.ReadPosition(tx)
		if err != nil {
			return ReadResult{}, err
		}

		if from.IsAfter(head) {
			return ReadPreparationState{from, head, nil}, nil
		}

		//
		exclusiveEnd := from.NextN(length).OrLower(head).Next()
		keyRange := fdb.KeyRange{
			Begin: stream.PositionToBlockIndex().Position(from),
			End:   stream.PositionToBlockIndex().Position(exclusiveEnd),
		}
		keys, err := tx.GetRange(keyRange, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).GetSliceWithError()
		if err != nil {
			return nil, errors.Wrap(err, "get message range failed")
		}

		pointers := make([]MessagePointer, len(keys))
		for n, kv := range keys {
			var pointer MessagePointer
			if _, err := xdr.Unmarshal(bytes.NewReader(kv.Value), &pointer); err != nil {
				return ReadPreparationState{}, errors.Wrap(err, fmt.Sprintf("error unmarshalling BlockMessagePointer at %v", kv.Key))
			}

			pointers[n] = pointer
		}

		return ReadPreparationState{
			From:     from,
			Pointers: pointers,
			Head:     head,
		}, err
	})
	if err != nil {
		this.log.Debug("pre phase failed", zap.Error(err))
		return ReadPreparationState{}, err
	}

	typed := result.(ReadPreparationState)
	this.log.Debug("pre phase success", zap.Any("result", typed))

	return typed, err
}

func chunkMessagePointers(log *zap.Logger, pointers []MessagePointer) [][]MessagePointer {
	left := pointers
	position := 0
	chunks := make([][]MessagePointer, 0)

	for len(left) != 0 {
		take := 0
		size := 0 * datasize.B

		for _, pointer := range left {
			growth := datasize.ByteSize(pointer.HeaderSize + pointer.ValueSize)
			log.Debug("next message", zap.Stringer("growth", growth))
			if (size + growth) > CHUNK_LIMIT {
				if take == 0 {
					panic("take should never be 0")
				}
				break
			}

			take++
			size += growth
		}

		chunks = append(chunks, left[0:take])
		left = left[take:]
		position += take
	}
	return chunks
}

type MessagesOrError struct {
	Messages []Message
	Error    error
}

func (this *FdbStreams) Read(id StreamId, from StreamPosition, length int) (ReadResult, error) {
	if ce := this.log.Check(zap.DebugLevel, "preparing to read from stream"); ce != nil {
		ce.Write(zap.Stringer("stream", id),
			zap.Stringer("from", from),
			zap.Int("length", length))
	}
	stream := this.rootSpace.Stream(id)

	scan, err := this.pre(stream, from, length)
	if err != nil {
		this.log.Debug("read pre scan failed", zap.Stringer("id", id), zap.Stringer("from", from), zap.Int("length", length), zap.Error(err))
		return ReadResult{}, err
	}

	if ce := this.log.Check(zap.DebugLevel, "pre scan success"); ce != nil {
		ce.Write(zap.Any("result", scan))
	}

	if ce := this.log.Check(zap.DebugLevel, "preparing to read from stream"); ce != nil {
		ce.Write(zap.Stringer("stream", id),
			zap.Stringer("from", from),
			zap.Int("length", length))
	}

	if len(scan.Pointers) == 0 {
		return ReadResult{
			Stream:   id,
			From:     from,
			Next:     0,
			HasNext:  false,
			Messages: make([]Message, 0),
		}, nil
	}

	done := make(chan struct{})
	complete := make(chan MessagesOrError)
	defer close(done)
	defer close(complete)

	chunks := chunkMessagePointers(this.log, scan.Pointers)
	totalMessages := 0
	for _, c := range chunks {
		go func(pointers []MessagePointer) {
			msgs, err := this.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
				// TODO do not read in a blocking fashion
				messages := make([]Message, len(pointers))
				for i, ptr := range pointers {
					msgSpace := this.rootSpace.Block(ptr.BlockId).Message(ptr.MessageIndex)
					header, err := msgSpace.Header().Read(tx)
					if err != nil {
						return nil, errors.Wrap(err, "header read failed")
					}
					value, err := msgSpace.Value().Read(tx)
					if err != nil {
						return nil, errors.Wrap(err, "value read failed")
					}

					messages[i] = Message{
						Header:  header,
						Payload: value,
					}
				}

				return messages, nil
			})

			var typed []Message
			if err == nil {
				typed = msgs.([]Message)
			}

			select {
			case complete <- MessagesOrError{typed, err}:
			case <-done:
			}
		}(c)

		totalMessages += len(c)
	}

	messages := make([]Message, 0, totalMessages)
	for range chunks {
		result := <-complete
		if result.Error != nil {
			return ReadResult{}, errors.Wrap(result.Error, "chunk read failed")
		}

		messages = append(messages, result.Messages...)
	}

	next := from.NextN(len(messages))
	hasNext := scan.Head.IsAfter(next)
	return ReadResult{id, from, next, hasNext, messages}, nil
}
