//go:generate gorunpkg github.com/99designs/gqlgen

package management

import (
	context "context"

	"github.com/pjvds/streamsdb/storage"
)

type Resolver struct {
	Store storage.FdbStreams
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
		Pos: int(result),
	}, nil
}

type queryResolver struct{ *Resolver }

func (r *queryResolver) Messages(ctx context.Context, id string, pos int, n *int) (*MessagesResult, error) {
	streamId := storage.StreamId(id)
	streamPos := storage.StreamPosition(pos)
	streamN := 10
	if n != nil {
		streamN = *n
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

	return &MessagesResult{
		From:     int(result.From),
		Messages: messages,
	}, nil
}
