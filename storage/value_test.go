package storage

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/c2h5oh/datasize"
	"github.com/pjvds/randombytes"
	"github.com/stretchr/testify/assert"
)

func TestValueRoundtrip(t *testing.T) {
	space := subspace.Sub("test", t.Name())
	db := fdb.MustOpenDefault()

	//nolint:errcheck
	defer db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		tx.ClearRange(space)
		return nil, nil
	})

	sizes := []datasize.ByteSize{
		1 * datasize.B,
		10 * datasize.B,
		500 * datasize.B,
		1 * datasize.KB,
		10 * datasize.KB,
		500 * datasize.KB,
		1 * datasize.MB,
		8 * datasize.MB,
	}

	for _, size := range sizes {
		t.Run(size.String(), func(t *testing.T) {
			space = space.Sub(t.Name())
			valueSpace := ValueSpace{space}
			write := randombytes.Make(int(size))

			if _, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				valueSpace.Set(tx, write)
				return nil, nil
			}); err != nil {
				t.Fatal(err)
			}

			read, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				read, err := valueSpace.Read(tx)
				return read, err
			})
			assert.NoError(t, err)
			assert.Equal(t, write, read)
		})
	}
}
