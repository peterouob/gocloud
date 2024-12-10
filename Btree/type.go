package Btree

type Int interface {
	int | int8 | int16 | int32 | int64
}

type is interface {
	Int | string
}

const MAX = 3
