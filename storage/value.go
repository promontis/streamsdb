package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/c2h5oh/datasize"
	"github.com/pkg/errors"
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
	const metadataSize = 7

	// write single value
	if len(value) <= ValuePartLimit {
		buf := make([]byte, len(value)+1)
		buf[0] = SingleValue
		copy(buf[1:], value)

		tx.Set(this.Sub(0), buf)
		return
	}

	// write multi value
	i := 1
	buf := value[ValuePartLimit-metadataSize:]

	// first write all values, except the first one
	for len(buf) > 0 {
		take := ValuePartLimit
		if take > len(buf) {
			take = len(buf)
		}

		tx.Set(this.Sub(i), buf[0:take])
		buf = buf[take:]
		i++
	}

	// now we know how much values we have written, so
	// write the first value including the meta data
	first := make([]byte, ValuePartLimit)
	copy(first[metadataSize:], value)
	first[0] = MultiValue
	binary.BigEndian.PutUint16(first[1:], uint16(i))
	binary.BigEndian.PutUint32(first[3:], uint32(len(value)))

	tx.Set(this.Sub(0), first)
}

// Read read a value from the value space. It handles single and
// multipart values and is optimized to want the full value at once.
func (this ValueSpace) Read(tx fdb.ReadTransaction) ([]byte, error) {
	if firstValue := tx.Get(this.Sub(0)).MustGet(); len(firstValue) > 0 {
		valueType := firstValue[0]

		if valueType == SingleValue {
			return firstValue[1:], nil
		}
		if valueType != MultiValue {
			return nil, errors.New("unexpected first value byte")
		}

		n := int(binary.BigEndian.Uint16(firstValue[1:]))
		s := int(binary.BigEndian.Uint32(firstValue[3:]))

		value := append(make([]byte, 0, s), firstValue[7:]...)

		keyRange := fdb.KeyRange{this.Sub(1), this.Sub(n + 1)}
		result := tx.GetRange(keyRange, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll})
		kvs, err := result.GetSliceWithError()

		if err != nil {
			return nil, errors.Wrap(err, "get range failed")
		}

		for _, kv := range kvs {
			value = append(value, kv.Value...)
		}
		if s != len(value) {
			return nil, errors.New(fmt.Sprintf("read short: %v instead of %v", len(value), s))
		}

		return value, nil
	}
	return nil, errors.New("not found")
}
