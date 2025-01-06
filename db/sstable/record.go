package sstable

import (
	"bytes"
)

type Record struct {
	Key   []byte
	Value []byte
	Idx   int
	next  *Record
}

func (r *Record) push(key, value []byte, idx int) (*Record, int) {
	h := r
	cur := r
	var prev *Record
	for {
		if cur == nil {
			if prev != nil {
				prev.next = &Record{Key: key, Value: value, Idx: idx}
			} else {
				h = &Record{Key: key, Value: value, Idx: idx}
			}
			break
		}

		cmp := bytes.Compare(key, cur.Key)
		if cmp == 0 {
			if idx >= r.Idx {
				oldIdx := cur.Idx
				cur.Key = key
				cur.Value = value
				cur.Idx = idx
				return h, oldIdx
			} else {
				return h, idx
			}
		} else if cmp < 0 {
			if prev != nil {
				prev.next = &Record{Key: key, Value: value, Idx: idx}
			} else {
				h = &Record{Key: key, Value: value, Idx: idx}
			}
			break
		} else {
			prev = cur
			cur = cur.next
		}
	}
	return h, -1
}

func (r *Record) Fill(source []*Node, idx int) *Record {
	record := r
	k, v := source[idx].nextRecord()
	if k != nil {
		record, idx = record.push(k, v, idx)
		for idx > -1 {
			k, v := source[idx].nextRecord()
			if k != nil {
				record, idx = record.push(k, v, idx)
			} else {
				idx = -1
			}
		}
	}
	return record
}
