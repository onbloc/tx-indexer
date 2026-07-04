package fetch

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	bft_types "github.com/gnolang/gno/tm2/pkg/bft/types"
	"go.uber.org/zap"

	"github.com/gnolang/tx-indexer/storage"
	storageErrors "github.com/gnolang/tx-indexer/storage/errors"
)

// blockHeightScanner is an optional storage capability: iterate stored block
// heights straight from the keys, without decoding block values.
type blockHeightScanner interface {
	BlockHeightIterator(fromBlockNum, toBlockNum uint64) (storage.Iterator[uint64], error)
}

// blockHeights iterates stored block heights in [from, to], preferring the
// cheap key-only scan and falling back to decoding blocks when unavailable.
func (f *Fetcher) blockHeights(from, to uint64) (storage.Iterator[uint64], error) {
	if s, ok := f.storage.(blockHeightScanner); ok {
		return s.BlockHeightIterator(from, to)
	}

	it, err := f.storage.BlockIterator(from, to)
	if err != nil {
		return nil, err
	}

	return &decodingHeightIter{it: it}, nil
}

// decodingHeightIter is the blockHeights fallback: it reads heights by decoding
// each block from an ordinary BlockIterator.
type decodingHeightIter struct {
	it storage.Iterator[*bft_types.Block]
}

func (d *decodingHeightIter) Next() bool { return d.it.Next() }

func (d *decodingHeightIter) Value() (uint64, error) {
	block, err := d.it.Value()
	if err != nil {
		return 0, err
	}

	return uint64(block.Height), nil
}

func (d *decodingHeightIter) Error() error { return d.it.Error() }
func (d *decodingHeightIter) Close() error { return d.it.Close() }

// maxAuditGaps caps how many missing heights an audit enqueues in one pass, so
// a badly corrupted or near-empty store can't blow up memory. The rest is
// picked up on a later run.
const maxAuditGaps = 100_000

// gapTracker is a thread-safe, de-duplicated set of block heights pending
// backfill (because they failed to fetch or to save).
type gapTracker struct {
	mu  sync.Mutex
	set map[uint64]struct{}
}

func newGapTracker() *gapTracker {
	return &gapTracker{
		set: make(map[uint64]struct{}),
	}
}

func (g *gapTracker) add(heights ...uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, h := range heights {
		g.set[h] = struct{}{}
	}
}

func (g *gapTracker) remove(height uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.set, height)
}

// snapshot returns the queued heights, sorted ascending.
func (g *gapTracker) snapshot() []uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	heights := make([]uint64, 0, len(g.set))
	for h := range g.set {
		heights = append(heights, h)
	}

	sort.Slice(heights, func(i, j int) bool {
		return heights[i] < heights[j]
	})

	return heights
}

// len returns the number of queued heights.
func (g *gapTracker) len() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return len(g.set)
}

// runBackfiller is the background loop that repairs gaps in the indexed data:
// an optional one-time storage audit on startup, then periodic draining of the
// queued heights (re-fetching only the missing blocks).
func (f *Fetcher) runBackfiller(ctx context.Context) {
	if f.auditOnStart {
		if err := f.auditGaps(ctx); err != nil && !errors.Is(err, context.Canceled) {
			f.logger.Error("gap audit failed", zap.Error(err))
		}
	}

	if f.txAudit {
		if err := f.auditTxGaps(ctx); err != nil && !errors.Is(err, context.Canceled) {
			f.logger.Error("tx gap audit failed", zap.Error(err))
		}
	}

	// Drain anything the audit (or the fetch loop) has already queued
	f.drainGaps(ctx)

	ticker := time.NewTicker(f.backfillInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			f.logger.Info("Backfiller service shut down")

			return
		case <-ticker.C:
			f.drainGaps(ctx)
		}
	}
}

// auditGaps queues every missing height between genesis and the latest saved
// height, recovering gaps that predate this process (e.g. blocks dropped by an
// older, buggy build).
func (f *Fetcher) auditGaps(ctx context.Context) error {
	latest, err := f.storage.GetLatestHeight()
	if err != nil {
		if errors.Is(err, storageErrors.ErrNotFound) {
			// Nothing indexed yet, nothing to audit
			return nil
		}

		return fmt.Errorf("unable to read latest height for audit: %w", err)
	}

	it, err := f.blockHeights(0, latest)
	if err != nil {
		return fmt.Errorf("unable to open block height iterator for audit: %w", err)
	}
	defer it.Close()

	var (
		expected uint64 // next height we expect to see
		queued   int
	)

	for it.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		height, err := it.Value()
		if err != nil {
			return fmt.Errorf("unable to read block height during audit: %w", err)
		}

		// Any heights between what we expected and this block are missing
		for h := expected; h < height && queued < maxAuditGaps; h++ {
			f.gaps.add(h)
			queued++
		}

		expected = height + 1
	}

	if err := it.Error(); err != nil {
		return fmt.Errorf("block height iterator error during audit: %w", err)
	}

	// Any heights between the last stored block and the latest height are missing
	for h := expected; h <= latest && queued < maxAuditGaps; h++ {
		f.gaps.add(h)
		queued++
	}

	if queued > 0 {
		f.logger.Warn(
			"gap audit found missing blocks, queued for backfill",
			zap.Int("count", queued),
			zap.Uint64("latest", latest),
		)

		if queued >= maxAuditGaps {
			f.logger.Warn(
				"gap audit hit the enqueue cap; remaining gaps will be picked up on a later run",
				zap.Int("cap", maxAuditGaps),
			)
		}
	}

	return nil
}

// auditTxGaps queues every height whose stored transaction count is below the
// block's declared NumTxs, recovering blocks saved without (some of) their txs.
// It walks the block and transaction iterators together in a single height-
// ordered pass. Decoding every block to read NumTxs makes it far more expensive
// than auditGaps, hence opt-in.
func (f *Fetcher) auditTxGaps(ctx context.Context) error {
	latest, err := f.storage.GetLatestHeight()
	if err != nil {
		if errors.Is(err, storageErrors.ErrNotFound) {
			return nil
		}

		return fmt.Errorf("unable to read latest height for tx audit: %w", err)
	}

	blockIt, err := f.storage.BlockIterator(0, latest)
	if err != nil {
		return fmt.Errorf("unable to open block iterator for tx audit: %w", err)
	}
	defer blockIt.Close()

	txIt, err := f.storage.TxIterator(0, latest, 0, math.MaxUint32)
	if err != nil {
		return fmt.Errorf("unable to open tx iterator for tx audit: %w", err)
	}
	defer txIt.Close()

	// Prime the tx iterator with its first transaction (if any)
	txHeight, txValid, err := nextTxHeight(txIt)
	if err != nil {
		return err
	}

	queued := 0

	for blockIt.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		block, err := blockIt.Value()
		if err != nil {
			return fmt.Errorf("unable to decode block during tx audit: %w", err)
		}

		if block.NumTxs == 0 {
			continue
		}

		height := uint64(block.Height)

		// Skip any stray transactions that sit below this block's height
		for txValid && txHeight < height {
			if txHeight, txValid, err = nextTxHeight(txIt); err != nil {
				return err
			}
		}

		// Count the transactions stored for this height
		var stored int64
		for txValid && txHeight == height {
			stored++

			if txHeight, txValid, err = nextTxHeight(txIt); err != nil {
				return err
			}
		}

		if stored < block.NumTxs && queued < maxAuditGaps {
			f.gaps.add(height)
			queued++
		}
	}

	if err := blockIt.Error(); err != nil {
		return fmt.Errorf("block iterator error during tx audit: %w", err)
	}

	if err := txIt.Error(); err != nil {
		return fmt.Errorf("tx iterator error during tx audit: %w", err)
	}

	if queued > 0 {
		f.logger.Warn(
			"tx audit found blocks with missing transactions, queued for backfill",
			zap.Int("count", queued),
			zap.Uint64("latest", latest),
		)
	}

	return nil
}

// nextTxHeight advances the transaction iterator and returns the height of the
// next transaction, whether one exists, and any error encountered.
func nextTxHeight(txIt storage.Iterator[*bft_types.TxResult]) (uint64, bool, error) {
	if !txIt.Next() {
		return 0, false, nil
	}

	tx, err := txIt.Value()
	if err != nil {
		return 0, false, fmt.Errorf("unable to decode tx during tx audit: %w", err)
	}

	return uint64(tx.Height), true, nil
}

// drainGaps attempts to backfill every currently queued height. Heights that
// are successfully fetched and saved are removed from the queue; the rest stay
// queued for the next backfiller tick.
func (f *Fetcher) drainGaps(ctx context.Context) {
	heights := f.gaps.snapshot()
	if len(heights) == 0 {
		return
	}

	f.logger.Info("Backfilling missing blocks", zap.Int("count", len(heights)))

	var repaired int

	for _, height := range heights {
		if ctx.Err() != nil {
			return
		}

		if err := f.backfillBlock(ctx, height); err != nil {
			f.logger.Warn(
				"unable to backfill block, will retry",
				zap.Uint64("height", height),
				zap.Error(err),
			)

			continue
		}

		f.gaps.remove(height)
		repaired++
	}

	if repaired > 0 {
		f.logger.Info(
			"Backfilled missing blocks",
			zap.Int("repaired", repaired),
			zap.Int("remaining", f.gaps.len()),
		)
	}
}

// backfillBlock fetches a single height (reusing the retrying fetch path) and
// persists it without touching the latest-height pointer, so filling an old
// gap never regresses the fetcher's forward progress.
func (f *Fetcher) backfillBlock(ctx context.Context, height uint64) error {
	c, missing, err := fetchChunk(ctx, f.client, chunkRange{from: height, to: height}, f.retry, f.logger)
	if err != nil {
		return err
	}

	if len(missing) > 0 || len(c.blocks) == 0 {
		return fmt.Errorf("block %d still unavailable", height)
	}

	wb := f.storage.WriteBatch()

	if failed := f.persistChunk(wb, c); len(failed) > 0 {
		if rErr := wb.Rollback(); rErr != nil {
			return fmt.Errorf("block %d failed to save, and rollback failed: %w", height, rErr)
		}

		return fmt.Errorf("block %d failed to save", height)
	}

	if err := wb.Commit(); err != nil {
		return fmt.Errorf("unable to commit backfilled block %d: %w", height, err)
	}

	return nil
}
