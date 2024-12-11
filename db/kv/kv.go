package kv

const (
	SuccessFind = iota
	DeleteSuccess
	NotFound
)

type KV struct {
	Key    string
	Value  []byte
	Delete bool
}
