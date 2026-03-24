package fetch

import "go.uber.org/zap"

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
