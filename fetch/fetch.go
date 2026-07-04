package fetch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/gnolang/gno/gno.land/pkg/gnoland"
	"github.com/gnolang/gno/tm2/pkg/amino"
	bft_types "github.com/gnolang/gno/tm2/pkg/bft/types"
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

var errInvalidGenesisState = errors.New("invalid genesis state")

// Fetcher is an instance of the block indexer
// fetcher
type Fetcher struct {
	storage storage.Storage
	client  Client
	events  Events

	logger      *zap.Logger
	chunkBuffer *slots

	maxSlots        int
	maxChunkSize    int64
	latestChunkSize int

	queryInterval time.Duration // block query interval

	retry            retryConfig   // retry policy for failed block / tx fetches
	gaps             *gapTracker   // heights pending backfill (fetch or save failures)
	backfillInterval time.Duration // how often queued gaps are re-fetched
	auditOnStart     bool          // scan storage for missing-block gaps on startup
	txAudit          bool          // also scan for blocks with missing txs on startup (expensive)
	auditFromHeight  uint64        // lower bound for both audits (skip heights below it)
	txAuditWindow    int           // heights per tx-audit window (throttle + resume granularity)
	txAuditNap       time.Duration // pause between tx-audit windows (throttle)

	dbPath       string
	clearOnReset bool
	genesisURL   string // optional URL to download genesis.json as fallback
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

	return f
}

func (f *Fetcher) fetchGenesisData(ctx context.Context) error {
	_, err := f.storage.GetLatestHeight()
	// Possible cases:
	// - err is ErrNotFound: the storage is empty, we execute the rest of the routine and fetch+write genesis data
	// - err is nil: the storage has a latest height, this means at least the genesis data has been written,
	//   or some blocks past it, we do nothing and return nil
	// - err is something else: there has been a storage error, we do nothing and return this error
	if !errors.Is(err, storageErrors.ErrNotFound) {
		return err
	}

	f.logger.Info("Fetching genesis")
	block, err := getGenesisBlock(ctx, f.client)
	if err != nil {
		f.logger.Warn("RPC genesis fetch failed", zap.Error(err))

		if f.genesisURL == "" {
			return fmt.Errorf("failed to fetch genesis block via RPC and no genesis-url configured: %w", err)
		}

		f.logger.Info("Falling back to genesis URL", zap.String("url", f.genesisURL))

		block, err = f.getGenesisBlockFromURL(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch genesis block from URL: %w", err)
		}
	}

	results, err := f.client.GetBlockResults(ctx, 0)
	if err != nil {
		return fmt.Errorf("failed to fetch genesis results: %w", err)
	}

	if results.Results == nil {
		return errors.New("nil results")
	}

	txResults := make([]*bft_types.TxResult, len(block.Txs))

	for txIndex, tx := range block.Txs {
		result := &bft_types.TxResult{
			Height:   0,
			Index:    uint32(txIndex),
			Tx:       tx,
			Response: results.Results.DeliverTxs[txIndex],
		}

		txResults[txIndex] = result
	}

	s := &slot{
		chunk: &chunk{
			blocks:  []*bft_types.Block{block},
			results: [][]*bft_types.TxResult{txResults},
		},
		chunkRange: chunkRange{
			from: 0,
			to:   0,
		},
	}

	return f.writeSlot(s)
}

// FetchChainData starts the fetching process that indexes
// blockchain data
func (f *Fetcher) FetchChainData(ctx context.Context) error {
	// Attempt to fetch the genesis data
	if err := f.fetchGenesisData(ctx); err != nil {
		// We treat this error as soft, to ease migration, since
		// some versions of gno networks don't support this.
		// In the future, we should hard fail if genesis is not fetch-able
		f.logger.Error("unable to fetch genesis data", zap.Error(err))

		return err
	}

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
			close(collectorCh)

			return nil
		case <-ticker.C:
			if err := attemptRangeFetch(); err != nil {
				return err
			}
		case response := <-collectorCh:
			// Find the slot index.
			// The reason for this search, is because the underlying
			// slots are shifted constantly to accommodate new ranges,
			// so by the time a slot is fetched, its original
			// position is not guaranteed
			index := sort.Search(f.chunkBuffer.Len(), func(i int) bool {
				return f.chunkBuffer.getSlot(i).chunkRange.from >= response.chunkRange.from
			})

			if response.error != nil {
				f.logger.Error(
					"error encountered during chunk fetch",
					zap.String("error", response.error.Error()),
				)
			}

			// The chunk still advances the latest height (so a single bad
			// block can't stall the fetcher); queueing the missing heights lets
			// the backfiller revisit them instead of skipping them silently.
			if len(response.missingBlocks) > 0 {
				f.gaps.add(response.missingBlocks...)

				f.logger.Warn(
					"blocks missing after retries, queued for backfill",
					zap.Uint64s("heights", response.missingBlocks),
				)
			}

			// Save the chunk
			f.chunkBuffer.setChunk(index, response.chunk)

			for f.chunkBuffer.Len() > 0 {
				// Peek the next sequential slot
				item := f.chunkBuffer.getSlot(0)

				if item.chunk == nil {
					// Chunk not fetched yet, nothing to do
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
	failed := f.persistChunk(wb, s.chunk)

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
func (f *Fetcher) persistChunk(wb storage.Batch, c *chunk) []uint64 {
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

		f.events.SignalEvent(event)
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

func getGenesisBlock(ctx context.Context, client Client) (*bft_types.Block, error) {
	gblock, err := client.GetGenesis(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get genesis block: %w", err)
	}

	if gblock.Genesis == nil {
		return nil, errInvalidGenesisState
	}

	genesisState, ok := gblock.Genesis.AppState.(gnoland.GnoGenesisState)
	if !ok {
		return nil, fmt.Errorf("unknown genesis state kind '%T'", gblock.Genesis.AppState)
	}

	txs := make([]bft_types.Tx, len(genesisState.Txs))
	for i, tx := range genesisState.Txs {
		txs[i], err = amino.Marshal(tx.Tx)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal genesis tx: %w", err)
		}
	}

	block := &bft_types.Block{
		Header: bft_types.Header{
			NumTxs:   int64(len(txs)),
			TotalTxs: int64(len(txs)),
			Time:     gblock.Genesis.GenesisTime,
			ChainID:  gblock.Genesis.ChainID,
		},
		Data: bft_types.Data{
			Txs: txs,
		},
	}

	return block, nil
}

// getGenesisBlockFromURL downloads genesis.json from the configured URL
// and parses it into a Block. Streams to a temp file to handle very large
// genesis files (200-300MB+).
func (f *Fetcher) getGenesisBlockFromURL(ctx context.Context) (*bft_types.Block, error) {
	tmpFile, err := os.CreateTemp("", "genesis-*.json")
	if err != nil {
		return nil, fmt.Errorf("unable to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	// Download to temp file
	if err := f.downloadGenesis(ctx, tmpFile); err != nil {
		tmpFile.Close()
		return nil, fmt.Errorf("unable to download genesis.json: %w", err)
	}
	tmpFile.Close()

	data, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read downloaded genesis file: %w", err)
	}

	f.logger.Info("Genesis file downloaded", zap.Int("bytes", len(data)))

	// Newer gno networks may emit genesis tx metadata fields that this
	// indexer's pinned gno dependency does not yet know about (e.g. the
	// "source" field on test-13). amino.UnmarshalJSON rejects unknown JSON
	// fields outright, so strip any metadata key that GnoTxMetadata does not
	// declare before handing the payload to amino. This keeps the indexer
	// forward-compatible with genesis schema additions without a dep bump.
	data, err = sanitizeGenesisTxMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("unable to sanitize genesis.json: %w", err)
	}

	var genDoc bft_types.GenesisDoc
	if err := amino.UnmarshalJSON(data, &genDoc); err != nil {
		return nil, fmt.Errorf("unable to parse genesis.json: %w", err)
	}

	genesisState, ok := genDoc.AppState.(gnoland.GnoGenesisState)
	if !ok {
		return nil, fmt.Errorf("unknown genesis state kind '%T'", genDoc.AppState)
	}

	txs := make([]bft_types.Tx, len(genesisState.Txs))
	for i, tx := range genesisState.Txs {
		txs[i], err = amino.Marshal(tx.Tx)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal genesis tx: %w", err)
		}
	}

	block := &bft_types.Block{
		Header: bft_types.Header{
			NumTxs:   int64(len(txs)),
			TotalTxs: int64(len(txs)),
			Time:     genDoc.GenesisTime,
			ChainID:  genDoc.ChainID,
		},
		Data: bft_types.Data{
			Txs: txs,
		},
	}

	return block, nil
}

// downloadGenesis downloads the genesis file from the configured URL,
// streaming directly to the provided file to handle large payloads.
func (f *Fetcher) downloadGenesis(ctx context.Context, dest *os.File) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.genesisURL, nil)
	if err != nil {
		return fmt.Errorf("unable to create request: %w", err)
	}

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return fmt.Errorf("unable to call genesis URL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Use a 1MB buffer for efficient large file streaming
	buf := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(dest, resp.Body, buf); err != nil {
		return fmt.Errorf("unable to write genesis file: %w", err)
	}

	return nil
}

// sanitizeGenesisTxMetadata removes any per-tx metadata fields that the pinned
// gnoland.GnoTxMetadata type does not declare. amino.UnmarshalJSON rejects
// unknown JSON fields, so newer genesis files carrying extra metadata keys
// (e.g. "source") would otherwise fail to parse. Unknown keys are dropped
// rather than erroring, keeping the indexer forward-compatible. The rest of the
// document (txs, app_state, validators, ...) is preserved verbatim.
func sanitizeGenesisTxMetadata(data []byte) ([]byte, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("unable to decode genesis document: %w", err)
	}

	appStateRaw, ok := doc["app_state"]
	if !ok {
		// Nothing to sanitize; return the input untouched.
		return data, nil
	}

	var appState map[string]json.RawMessage
	if err := json.Unmarshal(appStateRaw, &appState); err != nil {
		return nil, fmt.Errorf("unable to decode app_state: %w", err)
	}

	txsRaw, ok := appState["txs"]
	if !ok {
		return data, nil
	}

	var txs []map[string]json.RawMessage
	if err := json.Unmarshal(txsRaw, &txs); err != nil {
		return nil, fmt.Errorf("unable to decode genesis txs: %w", err)
	}

	known := knownJSONFields(reflect.TypeOf(gnoland.GnoTxMetadata{}))

	changed := false
	for _, tx := range txs {
		metaRaw, ok := tx["metadata"]
		if !ok {
			continue
		}

		var meta map[string]json.RawMessage
		if err := json.Unmarshal(metaRaw, &meta); err != nil {
			return nil, fmt.Errorf("unable to decode tx metadata: %w", err)
		}

		for key := range meta {
			if !known[key] {
				delete(meta, key)
				changed = true
			}
		}

		cleaned, err := json.Marshal(meta)
		if err != nil {
			return nil, fmt.Errorf("unable to re-encode tx metadata: %w", err)
		}

		tx["metadata"] = cleaned
	}

	if !changed {
		// No unknown fields were present; avoid the cost of re-encoding the
		// (potentially very large) document.
		return data, nil
	}

	newTxs, err := json.Marshal(txs)
	if err != nil {
		return nil, fmt.Errorf("unable to re-encode genesis txs: %w", err)
	}
	appState["txs"] = newTxs

	newAppState, err := json.Marshal(appState)
	if err != nil {
		return nil, fmt.Errorf("unable to re-encode app_state: %w", err)
	}
	doc["app_state"] = newAppState

	return json.Marshal(doc)
}

// knownJSONFields returns the set of JSON field names declared by the given
// struct type, derived from its `json` struct tags. The set is used as an
// allow-list so that schema additions to the source type are picked up
// automatically without touching this code.
func knownJSONFields(t reflect.Type) map[string]bool {
	fields := make(map[string]bool, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if name == "" || name == "-" {
			name = t.Field(i).Name
		}
		fields[name] = true
	}

	return fields
}
