package storage

import "github.com/apple/foundationdb/bindings/go/src/fdb/subspace"

type MessageSpace struct{ subspace.Subspace }

func (this MessageSpace) Header() ValueSpace {
	return ValueSpace{this.Sub(ElemHeader)}
}

func (this MessageSpace) Value() ValueSpace {
	return ValueSpace{this.Sub(ElemValue)}
}
