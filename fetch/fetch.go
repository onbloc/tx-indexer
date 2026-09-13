package fetch

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"time"

	queue "github.com/madz-lab/insertion-queue"
	"go.uber.org/zap"

	"github.com/gnolang/tx-indexer/storage"
	storageErrors "github.com/gnolang/tx-indexer/storage/errors"
	"github.com/gnolang/tx-indexer/types"
)

const (
	DefaultMaxSlots     = 100
	DefaultMaxChunkSize = 100

	// DefaultBackfillInterval is how often the backfiller drains queued gaps
	// (heights that failed to fetch / save) and re-fetches them.
	DefaultBackfillInterval = 30 * time.Second

	// DefaultTxAuditWindow is how many heights the tx-completeness audit
	// processes before pausing (throttle + watermark granularity).
	DefaultTxAuditWindow = 20_000

	// DefaultTxAuditNap is the pause between tx-audit windows,
	// which caps how much of the CPU the audit takes on constrained deployments.
	DefaultTxAuditNap = 100 * time.Millisecond
)

// Fetcher is an instance of the block indexer
// fetcher
type Fetcher struct {
	storage     storage.Storage
	client      Client
	events      Events
	logger      *zap.Logger
	chunkBuffer *slots
	gaps        *gapTracker // heights pending backfill (fetch or save failures)

	// retrying holds the slot ranges with missing heights, mapped to whether
	// the refetch is waiting to be respawned (true) or already in flight
	// (false). A range leaves the map only once it has been fetched in full
	retrying map[chunkRange]bool

	dbPath string
	retry  retryConfig // retry policy for failed block / tx fetches

	latestChunkSize  int
	maxChunkSize     int64
	queryInterval    time.Duration // block query interval
	maxSlots         int
	backfillInterval time.Duration // how often queued gaps are re-fetched
	auditFromHeight  uint64        // lower bound for both audits (skip heights below it)
	txAuditWindow    int           // heights per tx-audit window (throttle + resume granularity)
	txAuditNap       time.Duration // pause between tx-audit windows (throttle)

	clearOnReset bool // wipe storage when the chain resets
	auditOnStart bool // scan storage for missing-block gaps on startup
	txAudit      bool // also scan for blocks with missing txs on startup (expensive)
	txAuditReset bool // ignore the persisted tx audit watermark
}

// New creates a new data fetcher instance
// that gets blockchain data from a remote chain
func New(
	storage storage.Storage,
	client Client,
	events Events,
	opts ...Option,
) *Fetcher {
	f := &Fetcher{
		storage:          storage,
		client:           client,
		events:           events,
		queryInterval:    1 * time.Second,
		logger:           zap.NewNop(),
		maxSlots:         DefaultMaxSlots,
		maxChunkSize:     DefaultMaxChunkSize,
		retry:            defaultRetryConfig,
		gaps:             newGapTracker(),
		backfillInterval: 0,     // disabled unless explicitly enabled (see WithBackfillInterval)
		auditOnStart:     false, // enabled alongside the backfiller in production wiring
		txAudit:          false, // expensive tx-completeness scan, opt-in only
		txAuditWindow:    DefaultTxAuditWindow,
		txAuditNap:       DefaultTxAuditNap,
	}

	for _, opt := range opts {
		opt(f)
	}

	f.chunkBuffer = &slots{
		Queue:    make([]queue.Item, 0),
		maxSlots: f.maxSlots,
	}

	f.retrying = make(map[chunkRange]bool)

	return f
}

// FetchChainData starts the fetching process that indexes
// blockchain data. The genesis block is not handled here — the genesis
// package bootstraps it into the storage before any service starts.
func (f *Fetcher) FetchChainData(ctx context.Context) error {
	// Start the backfiller. It repairs any gaps left behind by failed
	// fetches / saves (including pre-existing ones already in storage)
	// without blocking the forward-fetching loop.
	if f.backfillInterval > 0 {
		go f.runBackfiller(ctx)
	}

	collectorCh := make(chan *workerResponse, DefaultMaxSlots)

	// attemptRangeFetch compares local and remote state
	// and spawns workers to fetch chunks of the chain
	attemptRangeFetch := func() error {
		// Check if there are any free slots
		if f.chunkBuffer.Len() == f.maxSlots {
			// Currently no free slot exists
			return nil
		}

		// Fetch the latest saved height
		latestLocal, err := f.storage.GetLatestHeight()
		if err != nil && !errors.Is(err, storageErrors.ErrNotFound) {
			return fmt.Errorf("unable to fetch latest block height, %w", err)
		}

		// Fetch the latest block from the chain
		latestRemote, latestErr := f.client.GetLatestBlockNumber(ctx)
		if latestErr != nil {
			f.logger.Error("unable to fetch latest block number", zap.Error(latestErr))

			return nil
		}

		// Check if there is a block gap
		if latestRemote == latestLocal {
			// No gap, nothing to sync
			return nil
		}

		// Check if there is reset chains
		if latestRemote < latestLocal {
			if f.clearOnReset {
				if err := os.RemoveAll(f.dbPath); err != nil {
					return fmt.Errorf("unable to remove DB, %w", err)
				}

				return fmt.Errorf("reset chain: latestRemote(%d) < latestLocal(%d)", latestRemote, latestLocal)
			}

			return nil
		}

		gaps := f.chunkBuffer.reserveChunkRanges(
			latestLocal+1,
			latestRemote,
			f.maxChunkSize,
		)

		for _, gap := range gaps {
			f.logger.Info(
				"Fetching range",
				zap.Uint64("from", gap.from),
				zap.Uint64("to", gap.to),
			)

			// Spawn worker
			info := &workerInfo{
				chunkRange: gap,
				resCh:      collectorCh,
				retry:      f.retry,
				logger:     f.logger,
			}

			go handleChunk(ctx, f.client, info)
		}

		return nil
	}

	// refetchFailedRanges respawns workers for only the heights still missing
	// from incomplete slots. The already fetched blocks stay in the slot, and
	// the write loop cannot advance past it until the slot is complete
	refetchFailedRanges := func() {
		for gap, queued := range f.retrying {
			if !queued {
				// Already back in flight
				continue
			}

			index := f.chunkBuffer.findSlot(gap.from)
			if index < 0 {
				delete(f.retrying, gap)

				continue
			}

			f.retrying[gap] = false

			for _, r := range contiguousRanges(f.chunkBuffer.getSlot(index).missing) {
				f.logger.Info(
					"Refetching missing blocks",
					zap.Uint64("from", r.from),
					zap.Uint64("to", r.to),
				)

				// Spawn worker
				info := &workerInfo{
					chunkRange: r,
					resCh:      collectorCh,
					retry:      f.retry,
					logger:     f.logger,
				}

				go handleChunk(ctx, f.client, info)
			}
		}
	}

	// Start a listener for monitoring new blocks
	ticker := time.NewTicker(f.queryInterval)
	defer ticker.Stop()

	// Execute the initial "catch up" with the chain
	if err := attemptRangeFetch(); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			f.logger.Info("Fetcher service shut down")

			// The channel is left open: workers still in flight deliver into
			// its buffer (or bail out on the cancelled context) and exit
			return nil
		case <-ticker.C:
			refetchFailedRanges()

			if err := attemptRangeFetch(); err != nil {
				return err
			}
		case response := <-collectorCh:
			// Find the slot owning the response. The underlying slots are
			// shifted constantly to accommodate new ranges, and a refetch
			// covers only a sub-range, so the slot is looked up by height
			index := f.chunkBuffer.findSlot(response.chunkRange.from)
			if index < 0 {
				f.logger.Warn(
					"dropping response for unknown range",
					zap.Uint64("from", response.chunkRange.from),
					zap.Uint64("to", response.chunkRange.to),
				)

				continue
			}

			item := f.chunkBuffer.getSlot(index)
			item.merge(response.chunk)

			if len(item.missing) > 0 {
				// The partial chunk stays in the slot and blocks the write loop
				// below, so the missing blocks are never skipped. Only they are
				// fetched again, instead of dropping and refetching the range
				f.logger.Warn(
					"blocks missing after retries, refetching",
					zap.Uint64s("heights", item.missing),
				)

				f.retrying[item.chunkRange] = true

				continue
			}

			delete(f.retrying, item.chunkRange)

			for f.chunkBuffer.Len() > 0 {
				// Peek the next sequential slot
				item := f.chunkBuffer.getSlot(0)

				if !item.complete() {
					// Chunk not fully fetched yet, nothing to do
					break
				}

				// Pop the next chunk
				f.chunkBuffer.PopFront()

				if err := f.writeSlot(item); err != nil {
					return err
				}
			}
		}
	}
}

func (f *Fetcher) writeSlot(s *slot) error {
	wb := f.storage.WriteBatch()

	// Save the fetched data
	failed := f.persistChunk(wb, s.chunk, true)

	f.logger.Info(
		"Added to batch block and tx data for range",
		zap.Uint64("from", s.chunkRange.from),
		zap.Uint64("to", s.chunkRange.to),
	)

	// Save the latest height data
	if err := wb.SetLatestHeight(s.chunkRange.to); err != nil {
		if rErr := wb.Rollback(); rErr != nil {
			return fmt.Errorf("unable to save latest height info, %w, %w", err, rErr)
		}

		return fmt.Errorf("unable to save latest height info, %w", err)
	}

	if err := wb.Commit(); err != nil {
		return fmt.Errorf("error persisting block information into storage, %w", err)
	}

	// Queue any block that failed to save for backfill. The latest height has
	// already advanced past them, so without this they would be lost forever.
	if len(failed) > 0 {
		f.gaps.add(failed...)

		f.logger.Warn(
			"blocks failed to save, queued for backfill",
			zap.Uint64s("heights", failed),
		)
	}

	f.latestChunkSize = len(s.chunk.blocks)

	return nil
}

// persistChunk writes the chunk's blocks and tx results into the provided
// batch and signals a NewBlock event for every successfully staged block.
// It returns the heights of blocks that failed to be staged so the caller can
// schedule them for backfill. The batch is not committed here.
func (f *Fetcher) persistChunk(wb storage.Batch, c *chunk, isSignalEvent bool) []uint64 {
	var failed []uint64

	for blockIndex, block := range c.blocks {
		if saveErr := wb.SetBlock(block); saveErr != nil {
			// This is a design choice that really highlights the strain
			// of keeping legacy testnets running. Current TM2 testnets
			// have blocks / transactions that are no longer compatible
			// with latest "master" changes for Amino, so these blocks / txs are ignored,
			// as opposed to this error being a show-stopper for the fetcher
			f.logger.Error("unable to save block", zap.String("err", saveErr.Error()))

			failed = append(failed, uint64(block.Height))

			continue
		}

		f.logger.Debug("Added block data to batch", zap.Int64("number", block.Height))

		// Get block results
		txResults := c.results[blockIndex]

		// Save the fetched transaction results
		txSaveFailed := false

		for _, txResult := range txResults {
			if err := wb.SetTx(txResult); err != nil {
				f.logger.Error("unable to  save tx", zap.String("err", err.Error()))

				txSaveFailed = true

				continue
			}

			f.logger.Debug(
				"Added tx to batch",
				zap.String("hash", base64.StdEncoding.EncodeToString(txResult.Tx.Hash())),
			)
		}

		// Block saved but some txs weren't: queue the height for backfill so
		// the missing txs are recovered instead of left incomplete.
		if txSaveFailed {
			failed = append(failed, uint64(block.Height))
		}

		// Alert any listeners of a new saved block
		event := &types.NewBlock{
			Block:   block,
			Results: txResults,
		}

		if isSignalEvent {
			f.events.SignalEvent(event)
		}
	}

	return failed
}

func (f *Fetcher) IsReady(ctx context.Context) (bool, error) {
	if f.latestChunkSize == int(f.maxChunkSize) {
		return false, fmt.Errorf("the data synchronization process is still in progress and hasn't "+
			"caught up with the current blockchain state. Chunk size: %d", f.latestChunkSize)
	}

	_, err := f.client.GetLatestBlockNumber(ctx)
	if err != nil {
		return false, fmt.Errorf("node RPC method is not reachable: %w", err)
	}

	return true, nil
}
