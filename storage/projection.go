package storage

/*
import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"go.uber.org/zap"
)

type ProjectionsSpace struct {
}

type Version int64

func (v Version) Next() Version {
	return v + 1
}

func (v Version) NextN(n int) Version {
	return Version(int64(v) + int64(n))
}

var NilVersion = Version(0)

type ProjectionDefinition struct {
	Name string
	Body string
	From []StreamId
}

type Projection struct {
	ProjectionDefinition
	FdbStreams

	Positions map[StreamId]StreamPosition
}

type ProjectionMessageHandler func(ReadResult)  {
}

type StreamAndMessages struct {
	StreamId StreamId
	From     int
	Messages []Message
}

type ProjectionSpace struct {
	subspace.Subspace
}

func (this Projection) scan(ids []StreamId) (map[StreamId]StreamPosition, error) {
	positions, err := this.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		positions := make(map[StreamId]StreamPosition, len(ids))
		for _, id := range ids {
			position, err := this.rootSpace.Stream(id).ReadPosition(tx)
			if err != nil {
				this.log.Debug("failed to read position from stream", zap.Stringer("stream", id), zap.Error(err))
				return nil, err
			}
			positions[id] = position
		}
		return positions, nil
	})

	if err != nil {
		return nil, err
	}

	return positions.(map[StreamId]StreamPosition), nil
}

func (this Projection) run(ctx FdbStreams) error {
	ctx.log.Debug("initializing projection", zap.String("name", this.Name))

	currentPositions, err := this.scan(this.From)
	if err != nil {
		return err
	}

	for streamId, pos := range this.Positions {
		if currentPositions[streamId].IsAfter(pos) {
		}
	}
}
*/
