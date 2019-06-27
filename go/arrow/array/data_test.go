package array_test

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewSliceData(t *testing.T) {
	data := []arrow.Time32{
		arrow.Time32(1),
		arrow.Time32(2),
		arrow.Time32(4),
		arrow.Time32(8),
		arrow.Time32(16),
	}

	dtype := arrow.FixedWidthTypes.Time32s
	fullSet := array.NewData(dtype, len(data),
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Time32Traits.CastToBytes(data))},
		nil, 0, 0,
	)
	defer fullSet.Release()

	// Test simple slice
	subSet := array.NewSliceData(fullSet, 0, 0)
	assert.Equal(t, subSet.Offset(), 0)
	assert.Equal(t, subSet.Len(), 0)
	assert.Equal(t, subSet.Buffers(), fullSet.Buffers())
	subSet.Release()

	subSet = array.NewSliceData(fullSet, 5, 5)
	assert.Equal(t, subSet.Offset(), 5)
	assert.Equal(t, subSet.Len(), 0)
	assert.Equal(t, subSet.Buffers(), fullSet.Buffers())
	subSet.Release()

	subSet = array.NewSliceData(fullSet, 0, 4)
	assert.Equal(t, subSet.Offset(), 0)
	assert.Equal(t, subSet.Len(), 4)
	assert.Equal(t, subSet.Buffers(), fullSet.Buffers())
	subSet.Release()

	subSet = array.NewSliceData(fullSet, 2, 5)
	assert.Equal(t, subSet.Offset(), 2)
	assert.Equal(t, subSet.Len(), 3)
	assert.Equal(t, subSet.Buffers(), fullSet.Buffers())

	// Test slice of slice
	refSubset := subSet

	subSet = array.NewSliceData(refSubset, 0, 0)
	assert.Equal(t, subSet.Offset(), 2)
	assert.Equal(t, subSet.Len(), 0)
	assert.Equal(t, subSet.Buffers(), fullSet.Buffers())
	subSet.Release()

	subSet = array.NewSliceData(refSubset, 3, 3)
	assert.Equal(t, subSet.Offset(), 5)
	assert.Equal(t, subSet.Len(), 0)
	assert.Equal(t, subSet.Buffers(), fullSet.Buffers())
	subSet.Release()

	subSet = array.NewSliceData(refSubset, 2, 3)
	assert.Equal(t, subSet.Offset(), 4)
	assert.Equal(t, subSet.Len(), 1)
	assert.Equal(t, subSet.Buffers(), fullSet.Buffers())
	subSet.Release()

	// Test failing
	// j out of range
	assert.PanicsWithValue(t, "arrow/array: index out of range", func() {
		array.NewSliceData(refSubset, 0, 4)
	})
	// i superior to j
	assert.PanicsWithValue(t, "arrow/array: index out of range", func() {
		array.NewSliceData(refSubset, 1, 0)
	})
}
