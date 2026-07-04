package fetch

import (
	"time"

	"go.uber.org/zap"
)

type Option func(f *Fetcher)

// WithLogger sets the logger to be used
// with the fetcher
func WithLogger(logger *zap.Logger) Option {
	return func(f *Fetcher) {
		f.logger = logger
	}
}

// WithMaxSlots sets the maximum worker slots
// for the fetcher
func WithMaxSlots(maxSlots int) Option {
	return func(f *Fetcher) {
		f.maxSlots = maxSlots
	}
}

// WithMaxChunkSize sets the maximum worker
// chunk size (data range) for the fetcher
func WithMaxChunkSize(maxChunkSize int64) Option {
	return func(f *Fetcher) {
		f.maxChunkSize = maxChunkSize
		f.latestChunkSize = int(maxChunkSize)
	}
}

// WithClearOnReset sets the clear on reset flag
func WithClearOnReset(clearOnReset bool) Option {
	return func(f *Fetcher) {
		f.clearOnReset = clearOnReset
	}
}

// WithDBPath sets the database path
func WithDBPath(dbPath string) Option {
	return func(f *Fetcher) {
		f.dbPath = dbPath
	}
}

// WithGenesisURL sets the fallback URL to download genesis.json
// when the RPC genesis call fails (e.g. for very large genesis files)
func WithGenesisURL(url string) Option {
	return func(f *Fetcher) {
		f.genesisURL = url
	}
}

// WithChunkFetchRetry configures how failed block / tx-result fetches are retried.
// Only the heights that fail are retried; already-fetched blocks are kept.
// maxRetries is the number of retry rounds, each preceded by delay.
func WithChunkFetchRetry(maxRetries int, delay time.Duration) Option {
	return func(f *Fetcher) {
		f.retry = retryConfig{
			maxRetries: maxRetries,
			retryDelay: delay,
		}
	}
}

// WithBackfillInterval sets how often the backfiller drains queued gaps.
// A non-positive interval disables the backfiller entirely.
func WithBackfillInterval(interval time.Duration) Option {
	return func(f *Fetcher) {
		f.backfillInterval = interval
	}
}

// WithStartupAudit controls whether the backfiller scans existing storage for
// missing-block gaps on startup.
func WithStartupAudit(enabled bool) Option {
	return func(f *Fetcher) {
		f.auditOnStart = enabled
	}
}

// WithTxAudit controls whether the startup audit also queues blocks
// that are stored but missing some or all of their transactions.
// It must decode every block to read the declared tx count,
// so it is much more expensive than the plain missing-block audit and is opt-in.
func WithTxAudit(enabled bool) Option {
	return func(f *Fetcher) {
		f.txAudit = enabled
	}
}
