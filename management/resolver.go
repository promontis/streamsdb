//go:generate gorunpkg github.com/99designs/gqlgen

package management

import (
	context "context"

	"github.com/pjvds/streamsdb/storage"
)

type Resolver struct {
	Store *storage.FdbStreams
}

func (r *Resolver) Mutation() MutationResolver {
	return &mutationResolver{r}
}
func (r *Resolver) Query() QueryResolver {
	return &queryResolver{r}
}

type mutationResolver struct{ *Resolver }

func (r *mutationResolver) Append(ctx context.Context, id string, messages []MessageInput) (*AppendResult, error) {
	streamId := storage.StreamId(id)
	streamMessages := make([]storage.Message, len(messages))

	for i, m := range messages {
		var header []byte
		var value []byte

		if m.Header != nil {
			header = []byte(*m.Header)
		}
		if m.Value != nil {
			value = []byte(*m.Value)
		}

		streamMessages[i] = storage.Message{header, value}
	}

	result, err := r.Store.Append(streamId, streamMessages...)
	if err != nil {
		return nil, err
	}

	return &AppendResult{
		From: int(result),
	}, nil
}

type queryResolver struct{ *Resolver }

func (r *queryResolver) Read(ctx context.Context, name string, from int, maxCount *int, direction *Direction) (*Slice, error) {
	streamId := storage.StreamId(name)
	streamPos := storage.StreamPosition(from)
	streamN := 10
	if maxCount != nil {
		streamN = *maxCount
	}

	result, err := r.Store.Read(streamId, streamPos, streamN)
	if err != nil {
		return nil, err
	}

	messages := make([]Message, len(result.Messages))
	for i, m := range result.Messages {
		messages[i] = Message{
			Header: string(m.Header),
			Value:  string(m.Payload),
		}
	}

	return &Slice{
		Stream:    name,
		From:      int(result.From),
		Count:     len(messages),
		Messages:  messages,
		Direction: DirectionForward,
	}, nil
}
