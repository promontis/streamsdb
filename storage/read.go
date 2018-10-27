package storage

import (
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/c2h5oh/datasize"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ReadResult struct {
	From     StreamPosition
	Next     StreamPosition
	HasNext  bool
	Messages []Message
}

type BlockMessagePointer struct {
	blockId      uuid.UUID
	messageIndex int
	messageSize  int
}

type ReadPreparationState struct {
	From     StreamPosition
	Head     StreamPosition
	Messages []BlockMessagePointer
}

func (this *FdbStreams) pre(stream StreamSpace, from StreamPosition, length int) (ReadPreparationState, error) {
	log := this.log.Named("pre")

	result, err := this.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		snapshot := tx.Snapshot()

		head, err := stream.ReadPosition(snapshot)
		if err != nil {
			return ReadResult{}, err
		}
		log.Debug("position read", zap.Stringer("head", head))

		if from.IsAfter(head) {
			return ReadPreparationState{from, head, nil}, nil
		}

		to := from.NextN(length).OrLower(head).Next()
		keyRange := fdb.KeyRange{
			stream.PositionToBlockIndex().Position(from),
			stream.PositionToBlockIndex().Position(to),
		}
		keys, err := snapshot.GetRange(keyRange, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).GetSliceWithError()
		if err != nil {
			return nil, errors.Wrap(err, "get message range failed")
		}

		messages := make([]BlockMessagePointer, len(keys))
		for n, kv := range keys {
			blockId := new(uuid.UUID)
			blockId.UnmarshalBinary(kv.Value[0:16])

			messageIndex := int(binary.BigEndian.Uint32(kv.Value[16:]))
			messageSize := int(binary.BigEndian.Uint32(kv.Value[24:]))
			messages[n] = BlockMessagePointer{*blockId, messageIndex, messageSize}
		}

		return ReadPreparationState{
			From:     from,
			Messages: messages,
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

func chunkMessagePointers(log *zap.Logger, pointers []BlockMessagePointer) [][]BlockMessagePointer {
	left := pointers
	position := 0
	chunks := make([][]BlockMessagePointer, 0)

	for len(left) != 0 {
		take := 0
		size := 0 * datasize.B

		for _, pointer := range left {
			growth := datasize.ByteSize(pointer.messageSize)
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
	stream := this.rootSpace.Stream(id)

	scan, err := this.pre(stream, from, length)
	if err != nil {
		return ReadResult{}, err
	}

	if len(scan.Messages) == 0 {
		return ReadResult{from, from, false, make([]Message, 0)}, nil
	}

	done := make(chan struct{})
	complete := make(chan MessagesOrError)
	defer close(done)
	defer close(complete)

	chunks := chunkMessagePointers(this.log, scan.Messages)
	totalMessages := 0
	for _, c := range chunks {
		go func(pointers []BlockMessagePointer) {
			msgs, err := this.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
				snapshot := tx.Snapshot()

				// TODO do not read in a blocking fashion
				messages := make([]Message, len(pointers))
				for i, ptr := range pointers {
					msgSpace := this.rootSpace.Block(ptr.blockId).Message(ptr.messageIndex)
					header, err := this.readValue(snapshot, msgSpace.Header())
					if err != nil {
						return nil, errors.Wrap(err, "header read failed")
					}
					value, err := this.readValue(snapshot, msgSpace.Value())
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

			select {
			case complete <- MessagesOrError{msgs.([]Message), err}:
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
	return ReadResult{from, next, hasNext, messages}, nil
}
