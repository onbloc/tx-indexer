package fetch

import (
	"testing"

	queue "github.com/madz-lab/insertion-queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlots_GetSlot(t *testing.T) {
	t.Parallel()

	ranges := []chunkRange{
		{
			from: 0,
			to:   1,
		},
		{
			from: 2,
			to:   3,
		},
		{
			from: 4,
			to:   5,
		},
	}

	s := &slots{
		make([]queue.Item, 0, len(ranges)),
		len(ranges),
	}

	for _, chunkRange := range ranges {
		s.Push(&slot{
			chunkRange: chunkRange,
		})
	}

	assert.Equal(t, s.Len(), len(ranges))

	for index, chunkRange := range ranges {
		slot := s.getSlot(index)

		assert.Equal(t, chunkRange, slot.chunkRange)
		assert.Nil(t, slot.chunk)
	}
}

func TestSlots_FindGaps(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name string

		existingRanges []chunkRange
		expectedRanges []chunkRange

		start        uint64
		end          uint64
		maxChunkSize int64
	}{
		{
			"no existing ranges",
			[]chunkRange{},
			[]chunkRange{
				{
					from: 1,
					to:   5,
				},
				{
					from: 6,
					to:   10,
				},
			},
			1,
			10,
			5,
		},
		{
			"existing later gaps",
			[]chunkRange{
				{
					from: 1,
					to:   5,
				},
				{
					from: 6,
					to:   10,
				},
			},
			[]chunkRange{
				{
					from: 11,
					to:   15,
				},
			},
			1,
			15,
			5,
		},
		{
			"existing middle gaps",
			[]chunkRange{
				{
					from: 1,
					to:   10,
				},
				{
					from: 20,
					to:   30,
				},
			},
			[]chunkRange{
				{
					from: 11,
					to:   19,
				},
			},
			1,
			30,
			10,
		},
		{
			"existing early gaps",
			[]chunkRange{
				{
					from: 20,
					to:   30,
				},
			},
			[]chunkRange{
				{
					from: 1,
					to:   10,
				},
				{
					from: 11,
					to:   19,
				},
			},
			1,
			30,
			10,
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			s := &slots{
				make([]queue.Item, 0, DefaultMaxSlots),
				DefaultMaxSlots,
			}

			for _, chunkRange := range testCase.existingRanges {
				s.Push(&slot{
					chunkRange: chunkRange,
				})
			}

			gaps := s.findGaps(
				testCase.start,
				testCase.end,
				testCase.maxChunkSize,
			)

			assert.Equal(t, testCase.expectedRanges, gaps)
		})
	}
}

func TestSlots_ReserveChunkRanges(t *testing.T) {
	t.Parallel()

	existingRanges := []chunkRange{
		{
			from: 11,
			to:   20,
		},
		{
			from: 31,
			to:   40,
		},
	}

	expectedRanges := []chunkRange{
		{
			from: 1,
			to:   10,
		},
		{
			from: 11,
			to:   20,
		},
		{
			from: 21,
			to:   30,
		},
		{
			from: 31,
			to:   40,
		},
		{
			from: 41,
			to:   50,
		},
	}

	// Create the slots queue
	s := &slots{
		make([]queue.Item, 0, 5),
		5,
	}

	for _, chunkRange := range existingRanges {
		s.Push(&slot{
			chunkRange: chunkRange,
		})
	}

	require.True(t, s.Len() == len(existingRanges))

	// Reserve chunk ranges
	s.reserveChunkRanges(1, 50, 10)

	require.Equal(t, len(expectedRanges), s.Len())

	for index, chunkRange := range expectedRanges {
		slot := s.getSlot(index)

		assert.Equal(t, chunkRange, slot.chunkRange)
	}

	// Sanity check for double reserves
	assert.Len(t, s.reserveChunkRanges(1, 50, 10), 0)
}

func TestSlot_Merge(t *testing.T) {
	t.Parallel()

	blocks := generateBlocks(t, 6, nil)
	mk := func(heights ...int) *chunk {
		c := &chunk{}
		for _, h := range heights {
			c.blocks = append(c.blocks, blocks[h])
			c.results = append(c.results, nil)
		}

		return c
	}

	s := &slot{chunkRange: chunkRange{from: 1, to: 5}}
	require.False(t, s.complete())

	s.merge(mk(1, 2, 5))
	assert.Equal(t, []uint64{3, 4}, s.missing)
	assert.False(t, s.complete())

	// Duplicates are ignored, new heights are merged in order
	s.merge(mk(2, 4))
	assert.Equal(t, []uint64{3}, s.missing)

	s.merge(mk(3))
	assert.Empty(t, s.missing)
	assert.True(t, s.complete())
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, blockHeightsOf(s.chunk.blocks))
	assert.Len(t, s.chunk.results, 5)
}

func TestSlots_FindSlot(t *testing.T) {
	t.Parallel()

	s := &slots{
		Queue:    make([]queue.Item, 0),
		maxSlots: 10,
	}

	s.Push(&slot{chunkRange: chunkRange{from: 1, to: 10}})
	s.Push(&slot{chunkRange: chunkRange{from: 21, to: 30}})

	assert.Equal(t, 0, s.findSlot(1))
	assert.Equal(t, 0, s.findSlot(10))
	assert.Equal(t, -1, s.findSlot(11))
	assert.Equal(t, 1, s.findSlot(25))
	assert.Equal(t, -1, s.findSlot(31))
}

func TestContiguousRanges(t *testing.T) {
	t.Parallel()

	assert.Nil(t, contiguousRanges(nil))
	assert.Equal(t,
		[]chunkRange{{from: 3, to: 5}, {from: 7, to: 7}, {from: 9, to: 10}},
		contiguousRanges([]uint64{3, 4, 5, 7, 9, 10}),
	)
}
