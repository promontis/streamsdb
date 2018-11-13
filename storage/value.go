package storage

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/c2h5oh/datasize"
)

const (
	SingleValue = byte(iota)
	MultiValue  = byte(iota)

	// the maximum size of a value stores in a value space
	ValuePartLimit = int(10 * datasize.KB)
)

type ValueSpace struct {
	subspace.Subspace
}

func (this ValueSpace) Set(tx fdb.Transaction, value []byte) {
	min := func(a, b int) int {
		if a < b {
			return a
		}
		return b
	}

	for i := 0; i < len(value); i += ValuePartLimit {
		to := min(i+ValuePartLimit, len(value))
		tx.Options().SetNextWriteNoWriteConflictRange()
		tx.Set(this.Pack(tuple.Tuple{i}), value[i:to])
	}
}

// Read read a value from the value space. It handles single and
// multipart values and is optimized to want the full value at once.
func (this ValueSpace) Read(tx fdb.ReadTransaction) ([]byte, error) {
	result := tx.GetRange(this, fdb.RangeOptions{
		Mode: fdb.StreamingModeWantAll,
	})

	kvs := result.GetSliceOrPanic()
	if len(kvs) == 0 {
		return make([]byte, 0), nil
	}
	if len(kvs) == 1 {
		return kvs[0].Value, nil
	}

	buffer := bytes.Buffer{}
	buffer.Grow(len(kvs) * ValuePartLimit)

	for _, kv := range kvs {
		buffer.Write(kv.Value)
	}

	return buffer.Bytes(), nil
}
