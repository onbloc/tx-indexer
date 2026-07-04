package fetch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	core_types "github.com/gnolang/gno/tm2/pkg/bft/rpc/core/types"
	"github.com/gnolang/gno/tm2/pkg/bft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientTypes "github.com/gnolang/tx-indexer/client/types"
	"github.com/gnolang/tx-indexer/internal/mock"
	"github.com/gnolang/tx-indexer/storage"
	storageErrors "github.com/gnolang/tx-indexer/storage/errors"
)

// fastRetry is a retry policy with a negligible delay, for tests.
var fastRetry = retryConfig{
	maxRetries: 5,
	retryDelay: time.Millisecond,
}

// erroringBatch returns a batch whose Execute always fails, forcing the
// sequential fetch fallback (which is the path that exercises retries).
func erroringBatch() clientTypes.Batch {
	return &mockBatch{
		executeFn: func(_ context.Context) ([]any, error) {
			return nil, errors.New("batch unavailable")
		},
		countFn: func() int { return 1 },
	}
}

// blocksAtHeights builds empty blocks at the given heights.
func blocksAtHeights(heights ...int64) []*types.Block {
	blocks := make([]*types.Block, len(heights))
	for i, h := range heights {
		blocks[i] = &types.Block{Header: types.Header{Height: h}}
	}

	return blocks
}

func TestGapTracker(t *testing.T) {
	t.Parallel()

	g := newGapTracker()
	require.Equal(t, 0, g.len())

	g.add(3, 1, 2, 1) // duplicate 1 collapses
	require.Equal(t, 3, g.len())
	assert.Equal(t, []uint64{1, 2, 3}, g.snapshot())

	g.remove(2)
	assert.Equal(t, []uint64{1, 3}, g.snapshot())
}

func TestAuditGaps(t *testing.T) {
	t.Parallel()

	testTable := []struct {
		name         string
		stored       []int64
		latest       uint64
		latestErr    error
		expectedGaps []uint64
	}{
		{"nothing indexed yet", nil, 0, storageErrors.ErrNotFound, []uint64{}},
		{"no gaps", []int64{0, 1, 2, 3}, 3, nil, []uint64{}},
		{"leading gap", []int64{2, 3}, 3, nil, []uint64{0, 1}},
		{"middle gaps", []int64{0, 1, 3, 5}, 5, nil, []uint64{2, 4}},
		{"trailing gap", []int64{0, 1, 2}, 5, nil, []uint64{3, 4, 5}},
		{"only genesis stored", []int64{0}, 3, nil, []uint64{1, 2, 3}},
	}

	for _, testCase := range testTable {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			storageMock := &mock.Storage{
				GetLatestSavedHeightFn: func() (uint64, error) {
					return testCase.latest, testCase.latestErr
				},
				BlockIteratorFn: func(_, _ uint64) (storage.Iterator[*types.Block], error) {
					return mock.NewSliceBlockIterator(blocksAtHeights(testCase.stored...)), nil
				},
			}

			f := New(storageMock, &mockClient{}, &mockEvents{})

			require.NoError(t, f.auditGaps(context.Background()))
			assert.Equal(t, testCase.expectedGaps, f.gaps.snapshot())
		})
	}
}

// heightScannerStorage is a storage that also implements blockHeightScanner,
// used to verify auditGaps prefers the cheap key-only scan.
type heightScannerStorage struct {
	*mock.Storage

	heightIter func(uint64, uint64) (storage.Iterator[uint64], error)
}

func (h *heightScannerStorage) BlockHeightIterator(from, to uint64) (storage.Iterator[uint64], error) {
	return h.heightIter(from, to)
}

// uint64SliceIter is an in-memory Iterator[uint64] for tests.
type uint64SliceIter struct {
	vals  []uint64
	index int
}

func newUint64SliceIter(vals []uint64) *uint64SliceIter {
	return &uint64SliceIter{vals: vals, index: -1}
}

func (it *uint64SliceIter) Next() bool             { it.index++; return it.index < len(it.vals) }
func (it *uint64SliceIter) Value() (uint64, error) { return it.vals[it.index], nil }
func (it *uint64SliceIter) Error() error           { return nil }
func (it *uint64SliceIter) Close() error           { return nil }

func TestAuditGaps_PrefersCheapHeightScan(t *testing.T) {
	t.Parallel()

	storageMock := &heightScannerStorage{
		Storage: &mock.Storage{
			GetLatestSavedHeightFn: func() (uint64, error) { return 3, nil },
			BlockIteratorFn: func(_, _ uint64) (storage.Iterator[*types.Block], error) {
				t.Fatal("BlockIterator must not be used when the cheap scan is available")

				return nil, nil
			},
		},
		heightIter: func(_, _ uint64) (storage.Iterator[uint64], error) {
			return newUint64SliceIter([]uint64{0, 2}), nil // missing 1 and 3
		},
	}

	f := New(storageMock, &mockClient{}, &mockEvents{})

	require.NoError(t, f.auditGaps(context.Background()))
	assert.Equal(t, []uint64{1, 3}, f.gaps.snapshot())
}

func TestAuditTxGaps(t *testing.T) {
	t.Parallel()

	type blockSpec struct {
		height int64
		numTxs int64
	}

	testTable := []struct {
		name         string
		blocks       []blockSpec
		txHeights    []int64 // one entry per stored tx, ascending
		latest       uint64
		latestErr    error
		expectedGaps []uint64
	}{
		{"nothing indexed yet", nil, nil, 0, storageErrors.ErrNotFound, []uint64{}},
		{
			"all complete",
			[]blockSpec{{1, 2}, {2, 1}},
			[]int64{1, 1, 2},
			2, nil, []uint64{},
		},
		{
			"partial and fully missing",
			[]blockSpec{{1, 2}, {2, 2}, {3, 2}},
			[]int64{1, 1, 2},
			3, nil, []uint64{2, 3},
		},
		{
			"empty blocks are skipped",
			[]blockSpec{{1, 0}, {2, 0}},
			nil,
			2, nil, []uint64{},
		},
		{
			"trailing block incomplete",
			[]blockSpec{{1, 1}, {2, 2}},
			[]int64{1, 2},
			2, nil, []uint64{2},
		},
	}

	for _, testCase := range testTable {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			blocks := make([]*types.Block, len(testCase.blocks))
			for i, spec := range testCase.blocks {
				blocks[i] = &types.Block{
					Header: types.Header{Height: spec.height, NumTxs: spec.numTxs},
				}
			}

			txs := make([]*types.TxResult, len(testCase.txHeights))
			for i, h := range testCase.txHeights {
				txs[i] = &types.TxResult{Height: h}
			}

			storageMock := &mock.Storage{
				GetLatestSavedHeightFn: func() (uint64, error) {
					return testCase.latest, testCase.latestErr
				},
				BlockIteratorFn: func(_, _ uint64) (storage.Iterator[*types.Block], error) {
					return mock.NewSliceBlockIterator(blocks), nil
				},
				TxIteratorFn: func(_, _ uint64, _, _ uint32) (storage.Iterator[*types.TxResult], error) {
					return mock.NewSliceTxIterator(txs), nil
				},
			}

			f := New(storageMock, &mockClient{}, &mockEvents{})

			require.NoError(t, f.auditTxGaps(context.Background()))
			assert.Equal(t, testCase.expectedGaps, f.gaps.snapshot())
		})
	}
}

func TestWriteSlot_QueuesFailedHeights(t *testing.T) {
	t.Parallel()

	txs := generateTransactions(t, 2)
	blocks := generateBlocks(t, 4, txs)

	testTable := []struct {
		name         string
		batch        func() storage.Batch
		expectedGaps []uint64
	}{
		{
			"block save fails",
			func() storage.Batch {
				return &mock.WriteBatch{
					SetBlockFn: func(*types.Block) error { return errors.New("amino incompatible") },
				}
			},
			[]uint64{3},
		},
		{
			"tx save fails",
			func() storage.Batch {
				return &mock.WriteBatch{
					SetTxFn: func(*types.TxResult) error { return errors.New("disk full") },
				}
			},
			[]uint64{3},
		},
		{
			"everything saves",
			func() storage.Batch { return &mock.WriteBatch{} },
			[]uint64{},
		},
	}

	for _, testCase := range testTable {
		testCase := testCase

		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			storageMock := &mock.Storage{GetWriteBatchFn: testCase.batch}

			f := New(storageMock, &mockClient{}, &mockEvents{})

			s := &slot{
				chunk: &chunk{
					blocks:  []*types.Block{blocks[3]},
					results: [][]*types.TxResult{{{Height: 3}, {Height: 3}}},
				},
				chunkRange: chunkRange{from: 3, to: 3},
			}

			require.NoError(t, f.writeSlot(s))
			assert.Equal(t, testCase.expectedGaps, f.gaps.snapshot())
		})
	}
}

func TestDrainGaps(t *testing.T) {
	t.Parallel()

	const txCount = 3

	txs := generateTransactions(t, txCount)
	blocks := generateBlocks(t, 6, txs)

	t.Run("backfills queued heights without advancing the latest height", func(t *testing.T) {
		t.Parallel()

		var (
			mu          sync.Mutex
			savedBlocks []*types.Block
			latestSet   bool
		)

		storageMock := &mock.Storage{
			GetWriteBatchFn: func() storage.Batch {
				return &mock.WriteBatch{
					SetBlockFn: func(b *types.Block) error {
						mu.Lock()
						savedBlocks = append(savedBlocks, b)
						mu.Unlock()

						return nil
					},
					SetLatestHeightFn: func(uint64) error {
						latestSet = true

						return nil
					},
				}
			},
		}

		client := &mockClient{
			createBatchFn: func() clientTypes.Batch { return erroringBatch() },
			getBlockFn: func(num uint64) (*core_types.ResultBlock, error) {
				return &core_types.ResultBlock{Block: blocks[num]}, nil
			},
			getBlockResultsFn: func(num uint64) (*core_types.ResultBlockResults, error) {
				return mockBlockResults(int64(num), txCount), nil
			},
		}

		f := New(storageMock, client, &mockEvents{}, WithChunkFetchRetry(2, time.Millisecond))
		f.gaps.add(2, 4)

		f.drainGaps(context.Background())

		mu.Lock()
		defer mu.Unlock()

		require.Len(t, savedBlocks, 2)
		assert.Equal(t, 0, f.gaps.len(), "queue should be drained")
		assert.False(t, latestSet, "backfill must not advance the latest height")
	})

	t.Run("keeps a height queued when it still cannot be fetched", func(t *testing.T) {
		t.Parallel()

		client := &mockClient{
			createBatchFn: func() clientTypes.Batch { return erroringBatch() },
			getBlockFn: func(uint64) (*core_types.ResultBlock, error) {
				return nil, errors.New("still down")
			},
		}

		f := New(&mock.Storage{}, client, &mockEvents{}, WithChunkFetchRetry(1, time.Millisecond))
		f.gaps.add(7)

		f.drainGaps(context.Background())

		assert.Equal(t, []uint64{7}, f.gaps.snapshot(), "unfetchable height must stay queued")
	})

	t.Run("keeps a height queued when the fetched block fails to save", func(t *testing.T) {
		t.Parallel()

		storageMock := &mock.Storage{
			GetWriteBatchFn: func() storage.Batch {
				return &mock.WriteBatch{
					SetBlockFn: func(*types.Block) error { return errors.New("amino incompatible") },
				}
			},
		}

		client := &mockClient{
			createBatchFn: func() clientTypes.Batch { return erroringBatch() },
			getBlockFn: func(num uint64) (*core_types.ResultBlock, error) {
				return &core_types.ResultBlock{Block: blocks[num]}, nil
			},
			getBlockResultsFn: func(num uint64) (*core_types.ResultBlockResults, error) {
				return mockBlockResults(int64(num), txCount), nil
			},
		}

		f := New(storageMock, client, &mockEvents{}, WithChunkFetchRetry(1, time.Millisecond))
		f.gaps.add(5)

		f.drainGaps(context.Background())

		assert.Equal(t, []uint64{5}, f.gaps.snapshot(), "unsavable height must stay queued")
	})
}

func TestRunBackfiller_AuditsThenBackfills(t *testing.T) {
	t.Parallel()

	const txCount = 1

	txs := generateTransactions(t, txCount)
	blocks := generateBlocks(t, 4, txs)

	// Stored: 0, 2 (missing 1), latest 2
	stored := blocksAtHeights(0, 2)

	var (
		mu    sync.Mutex
		saved []*types.Block
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storageMock := &mock.Storage{
		GetLatestSavedHeightFn: func() (uint64, error) { return 2, nil },
		BlockIteratorFn: func(_, _ uint64) (storage.Iterator[*types.Block], error) {
			return mock.NewSliceBlockIterator(stored), nil
		},
		GetWriteBatchFn: func() storage.Batch {
			return &mock.WriteBatch{
				SetBlockFn: func(b *types.Block) error {
					mu.Lock()
					saved = append(saved, b)
					mu.Unlock()

					cancel() // stop the backfiller once the gap is filled

					return nil
				},
			}
		},
	}

	client := &mockClient{
		createBatchFn: func() clientTypes.Batch { return erroringBatch() },
		getBlockFn: func(num uint64) (*core_types.ResultBlock, error) {
			return &core_types.ResultBlock{Block: blocks[num]}, nil
		},
		getBlockResultsFn: func(num uint64) (*core_types.ResultBlockResults, error) {
			return mockBlockResults(int64(num), txCount), nil
		},
	}

	f := New(
		storageMock,
		client,
		&mockEvents{},
		WithBackfillInterval(time.Hour), // ticker must not fire before ctx is cancelled
		WithStartupAudit(true),
		WithChunkFetchRetry(2, time.Millisecond),
	)

	f.runBackfiller(ctx)

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, saved, 1)
	assert.EqualValues(t, 1, saved[0].Height)
}
