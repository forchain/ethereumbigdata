package lib

import (
	"container/heap"
	"sort"
	"math/big"
)

// An bigIntHeap is a min-heap of ints.
type bigIntHeap []*big.Int

func (h bigIntHeap) Len() int           { return len(h) }
func (h bigIntHeap) Less(i, j int) bool { return h[i].Cmp(h[j]) < 0 }
func (h bigIntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *bigIntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*big.Int))
}

func (h *bigIntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0: n-1]
	return x
}

type TopList struct {
	capacity_ uint32
	heap_     bigIntHeap
}

func (_t *TopList) Init(_capacity uint32) {
	_t.capacity_ = _capacity
	heap.Init(&_t.heap_)
}

func (_t *TopList) Push(_x *big.Int) {
	if uint32(_t.heap_.Len()) >= _t.capacity_ {

		if _x.Cmp(_t.Min()) > 0 {
			heap.Push(&_t.heap_, _x)
			heap.Pop(&_t.heap_)
		}
	} else {
		heap.Push(&_t.heap_, _x)
	}
}

func (_t *TopList) Pop() big.Int {
	return heap.Pop(&_t.heap_).(big.Int)
}

func (_t *TopList) Len() int {
	return _t.heap_.Len()
}

func (_t *TopList) Min() *big.Int {
	return _t.heap_[0]
}

type float64Sorted []*big.Int

func (h float64Sorted) Len() int           { return len(h) }
func (h float64Sorted) Less(i, j int) bool { return h[i].Cmp(h[j]) > 0 }
func (h float64Sorted) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (_t *TopList) Sorted() []*big.Int {

	list := make(float64Sorted, 0)

	for _, v := range _t.heap_ {
		list = append(list, v)
	}

	sort.Sort(list)

	return list
}
