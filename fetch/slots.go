package fetch

import (
	"sort"

	"github.com/gnolang/gno/tm2/pkg/bft/types"
	queue "github.com/madz-lab/insertion-queue"
)

// chunk represents a single blockchain
// data range
type chunk struct {
	blocks  []*types.Block
	results [][]*types.TxResult // summarized results
}

// slot is a single chunk slot
type slot struct {
	chunk      *chunk     // retrieved data chunk
	chunkRange chunkRange // retrieved data chunk range

	// missing holds the heights of the range not yet present in chunk. A slot
	// is only written out once it is complete (chunk set and missing empty),
	// so the saved latest height never advances past an unfetched block
	missing []uint64
}

// complete returns true when the slot holds every block of its range
func (s *slot) complete() bool {
	return s.chunk != nil && len(s.missing) == 0
}

// merge adds the fetched blocks into the slot's chunk and recomputes the
// heights still missing from the range
func (s *slot) merge(c *chunk) {
	if s.chunk == nil {
		s.chunk = &chunk{}
	}

	have := make(map[uint64]struct{}, len(s.chunk.blocks)+len(c.blocks))
	for _, block := range s.chunk.blocks {
		have[uint64(block.Height)] = struct{}{}
	}

	for i, block := range c.blocks {
		height := uint64(block.Height)
		if _, ok := have[height]; ok {
			continue
		}

		have[height] = struct{}{}

		s.chunk.blocks = append(s.chunk.blocks, block)
		s.chunk.results = append(s.chunk.results, c.results[i])
	}

	sort.Sort(byHeight{s.chunk})

	s.missing = missingHeights(s.chunkRange, have)
}

// byHeight sorts a chunk's blocks (and their paired results) by height
type byHeight struct{ *chunk }

func (b byHeight) Len() int           { return len(b.blocks) }
func (b byHeight) Less(i, j int) bool { return b.blocks[i].Height < b.blocks[j].Height }
func (b byHeight) Swap(i, j int) {
	b.blocks[i], b.blocks[j] = b.blocks[j], b.blocks[i]
	b.results[i], b.results[j] = b.results[j], b.results[i]
}

func (s *slot) Less(i queue.Item) bool {
	// No need to check the type assertion
	other, _ := i.(*slot)

	return s.chunkRange.less(other.chunkRange)
}

// chunkRange is the data sequence range for the data
type chunkRange struct {
	from uint64 // sequence from (inclusive)
	to   uint64 // sequence to (inclusive)
}

// less returns a flag indicating if the current chunk range is less than the other
func (c chunkRange) less(other chunkRange) bool {
	return c.from < other.from
}

// slots is the fixed priority-queue slot representation
type slots struct {
	queue.Queue

	maxSlots int
}

// getSlot fetches the slot at the specific index
func (s *slots) getSlot(index int) *slot {
	if s.Len()-1 < index {
		return nil
	}

	return s.Index(index).(*slot)
}

// findSlot returns the index of the slot whose range contains the height,
// or -1 if no such slot is reserved
func (s *slots) findSlot(height uint64) int {
	index := sort.Search(s.Len(), func(i int) bool {
		return s.getSlot(i).chunkRange.to >= height
	})

	if index == s.Len() || s.getSlot(index).chunkRange.from > height {
		return -1
	}

	return index
}

// reserveChunkRanges reserves empty chunk ranges, and returns them, if any
func (s *slots) reserveChunkRanges(start, end uint64, maxChunkSize int64) []chunkRange {
	freeSlots := s.maxSlots - s.Len()

	gaps := s.findGaps(start, end, maxChunkSize)
	maxRanges := min(len(gaps), freeSlots)
	chunkRanges := make([]chunkRange, maxRanges)

	for index, gap := range gaps[:maxRanges] {
		chunkRanges[index] = gap

		s.Push(&slot{
			chunk:      nil,
			chunkRange: gap,
		})
	}

	return chunkRanges
}

// findGaps finds the chunk gaps in the specified range.
// The method finds any chunk ranges that can be filled between
// start and end (inclusive). This means that the calling process
// can potentially recover unfilled gaps, since they will exist given the
// proper start and end ranges. This situation is reflected in the testing
// suite for the slots
func (s *slots) findGaps(start, end uint64, maxSize int64) []chunkRange {
	var (
		chunkRanges []chunkRange // contains all gaps
		dividedGaps []chunkRange // contains at most maxSize gaps
	)

	if s.Len() == 0 {
		chunkRanges = append(chunkRanges, chunkRange{
			from: start,
			to:   end,
		})
	} else {
		prevTo := start - 1

		for _, partRaw := range s.Queue {
			part := partRaw.(*slot)

			if part.chunkRange.from > prevTo+1 {
				chunkRanges = append(chunkRanges, chunkRange{
					from: prevTo + 1,
					to:   part.chunkRange.from - 1,
				})
			}

			prevTo = part.chunkRange.to
		}

		if prevTo < end {
			chunkRanges = append(chunkRanges, chunkRange{
				from: prevTo + 1,
				to:   end,
			})
		}
	}

	for _, gap := range chunkRanges {
		if gap.to-gap.from+1 <= uint64(maxSize) {
			dividedGaps = append(dividedGaps, gap)

			continue
		}

		for i := gap.from; i <= gap.to; i += uint64(maxSize) {
			newGap := chunkRange{
				from: i,
				to:   i + uint64(maxSize) - 1,
			}

			if newGap.to > gap.to {
				newGap.to = gap.to
			}

			dividedGaps = append(dividedGaps, newGap)
		}
	}

	return dividedGaps
}
