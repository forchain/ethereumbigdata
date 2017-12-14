package lib

import (
	"container/heap"
	"sort"
)

// An float64Heap is a min-heap of ints.
type float64Heap []float64

func (h float64Heap) Len() int           { return len(h) }
func (h float64Heap) Less(i, j int) bool { return h[i] < h[j] }
func (h float64Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *float64Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(float64))
}

func (h *float64Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0: n-1]
	return x
}

type TopList struct {
	capacity_ uint32
	heap_     float64Heap
}

func (_t *TopList) Init(_capacity uint32) {
	_t.capacity_ = _capacity
	heap.Init(&_t.heap_)
}

func (_t *TopList) Push(_x float64) {
	if uint32(_t.heap_.Len()) >= _t.capacity_ {
		if _x > _t.Min() {
			heap.Push(&_t.heap_, _x)
			heap.Pop(&_t.heap_)
		}
	} else {
		heap.Push(&_t.heap_, _x)
	}
}

func (_t *TopList) Pop() float64 {
	return heap.Pop(&_t.heap_).(float64)
}

func (_t *TopList) Len() int {
	return _t.heap_.Len()
}

func (_t *TopList) Min() float64 {
	return _t.heap_[0]
}

type float64Sorted []float64

func (h float64Sorted) Len() int           { return len(h) }
func (h float64Sorted) Less(i, j int) bool { return h[i] > h[j] }
func (h float64Sorted) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (_t *TopList) Sorted() []float64 {

	list := make(float64Sorted, 0)

	for _, v := range _t.heap_ {
		list = append(list, v)
	}

	sort.Sort(list)

	return list
}
