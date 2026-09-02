package http

import (
	"net/url"
	"path"

	"github.com/GopeedLab/gopeed/internal/fetcher"
	"github.com/GopeedLab/gopeed/pkg/base"
	fhttp "github.com/GopeedLab/gopeed/pkg/protocol/http"
)

// ============================================================================
// Fetcher Data (for Store/Restore)
// ============================================================================

type fetcherData struct {
	Connections              []*connection
	RedirectURL              string // Saved redirect URL for resume
	IfRange                  string // Strong ETag or Last-Modified validator for safe resume
	RangeReprobeEligible     bool   // Origin advertised Range before falling back to sequential mode
	RangeValidatorPinned     bool   // Recovered Range mode must keep the same If-Range validator
	SequentialSizeUnknown    bool   // Interrupted chunked restart must retry from byte zero
	SequentialRestartPending bool   // Full response produced no bytes yet; retry must accept its current size
	RecoveryGeneration       uint64 // Destructive file/recovery-state epoch
	Range                    *bool  // Authoritative Range mode; nil for records saved by older versions
	ResourceSize             *int64 // Authoritative resource size from the same snapshot as Connections
	FileSize                 *int64 // Authoritative single-file size; nil for older records
}

func (fd *fetcherData) CheckpointGeneration() uint64 {
	return fd.RecoveryGeneration
}

// ============================================================================
// Fetcher Manager
// ============================================================================

type FetcherManager struct {
}

func (fm *FetcherManager) Name() string {
	return "http"
}

func (fm *FetcherManager) Filters() []*fetcher.SchemeFilter {
	return []*fetcher.SchemeFilter{
		{
			Type:    fetcher.FilterTypeUrl,
			Pattern: "HTTP",
		},
		{
			Type:    fetcher.FilterTypeUrl,
			Pattern: "HTTPS",
		},
	}
}

func (fm *FetcherManager) Build() fetcher.Fetcher {
	return &Fetcher{}
}

func (fm *FetcherManager) ParseName(u string) string {
	var name string
	url, err := url.Parse(u)
	if err != nil {
		return ""
	}
	name = path.Base(url.Path)
	if name == "" || name == "/" || name == "." {
		name = url.Hostname()
	}
	return name
}

func (fm *FetcherManager) AutoRename() bool {
	return true
}

func (fm *FetcherManager) DefaultConfig() any {
	return &config{
		UserAgent:   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
		Connections: 16,
	}
}

func (fm *FetcherManager) Store(f fetcher.Fetcher) (data any, err error) {
	_f := f.(*Fetcher)
	_f.redirectLock.Lock()
	redirectURL := _f.redirectURL
	_f.redirectLock.Unlock()

	// Build an immutable snapshot while holding the same lock used by download
	// workers to update Range mode and connection progress. The storage layer
	// marshals this value after Store returns, so returning live pointers would
	// otherwise race with the active download and could persist a mixed state.
	_f.connMu.Lock()
	connections := snapshotConnectionsLocked(_f.connections)
	ifRange := _f.ifRange
	rangeReprobeEligible := _f.rangeReprobeEligible
	rangeValidatorPinned := _f.rangeValidatorPinned
	sequentialSizeUnknown := _f.sequentialSizeUnknown
	sequentialRestartPending := _f.sequentialRestartPending
	recoveryGeneration := _f.recoveryGeneration.Load()
	var rangeMode *bool
	var resourceSize *int64
	var fileSize *int64
	if _f.meta != nil && _f.meta.Res != nil {
		value := _f.meta.Res.Range
		rangeMode = &value
		size := _f.meta.Res.Size
		resourceSize = &size
		if len(_f.meta.Res.Files) > 0 && _f.meta.Res.Files[0] != nil {
			size := _f.meta.Res.Files[0].Size
			fileSize = &size
		}
	}
	_f.connMu.Unlock()

	return &fetcherData{
		Connections:              connections,
		RedirectURL:              redirectURL,
		IfRange:                  ifRange,
		RangeReprobeEligible:     rangeReprobeEligible,
		RangeValidatorPinned:     rangeValidatorPinned,
		SequentialSizeUnknown:    sequentialSizeUnknown,
		SequentialRestartPending: sequentialRestartPending,
		RecoveryGeneration:       recoveryGeneration,
		Range:                    rangeMode,
		ResourceSize:             resourceSize,
		FileSize:                 fileSize,
	}, nil
}

// snapshotConnectionsLocked copies only the serialized connection state and
// deep-copies chunks. The caller must hold the fetcher's connMu.
func snapshotConnectionsLocked(connections []*connection) []*connection {
	snapshot := make([]*connection, len(connections))
	for i, conn := range connections {
		if conn == nil {
			continue
		}
		copyConn := &connection{
			ID:         conn.ID,
			Role:       conn.Role,
			State:      conn.State,
			Downloaded: conn.Downloaded,
			Completed:  conn.Completed,
		}
		if conn.Chunk != nil {
			copyChunk := *conn.Chunk
			copyConn.Chunk = &copyChunk
		}
		snapshot[i] = copyConn
	}
	return snapshot
}

func (fm *FetcherManager) Restore() (v any, f func(meta *fetcher.FetcherMeta, v any) fetcher.Fetcher) {
	return &fetcherData{}, func(meta *fetcher.FetcherMeta, v any) fetcher.Fetcher {
		fd := v.(*fetcherData)
		fb := &FetcherManager{}
		fetcher := fb.Build().(*Fetcher)
		fetcher.meta = meta
		base.ParseReqExtra[fhttp.ReqExtra](fetcher.meta.Req)
		base.ParseOptExtra[fhttp.OptsExtra](fetcher.meta.Opts)
		if len(fd.Connections) > 0 {
			fetcher.connections = fd.Connections
		}
		// Restore redirect URL for resume
		if fd.RedirectURL != "" {
			fetcher.redirectURL = fd.RedirectURL
		}
		fetcher.ifRange = fd.IfRange
		fetcher.rangeReprobeEligible = fd.RangeReprobeEligible
		fetcher.rangeValidatorPinned = fd.RangeValidatorPinned
		fetcher.sequentialSizeUnknown = fd.SequentialSizeUnknown
		fetcher.sequentialRestartPending = fd.SequentialRestartPending
		fetcher.recoveryGeneration.Store(fd.RecoveryGeneration)
		if fd.Range != nil && fetcher.meta.Res != nil {
			// Recovery-critical resource fields live in the same persisted snapshot
			// as Connections, making them authoritative over task metadata that may
			// have been saved just before or after this record.
			fetcher.meta.Res.Range = *fd.Range
		}
		if fd.ResourceSize != nil && fetcher.meta.Res != nil {
			fetcher.meta.Res.Size = *fd.ResourceSize
		}
		if fd.FileSize != nil && fetcher.meta.Res != nil && len(fetcher.meta.Res.Files) > 0 && fetcher.meta.Res.Files[0] != nil {
			fetcher.meta.Res.Files[0].Size = *fd.FileSize
		}
		if fetcher.meta.Res != nil && fetcher.meta.Res.Range && fetcher.ifRange == "" {
			// Old or invalidated checkpoints may claim ranged progress without a
			// representation validator. Discard every saved assignment and force an
			// authoritative full response; no existing prefix is safe to reuse.
			fetcher.meta.Res.Range = false
			fetcher.connections = nil
			fetcher.rangeReprobeEligible = false
			fetcher.rangeValidatorPinned = false
			fetcher.sequentialRestartPending = true
		}
		if fetcher.meta.Res != nil && !fetcher.meta.Res.Range && len(fd.Connections) == 0 {
			// A task record without its sequential checkpoint cannot prove that the
			// current target length belongs to the next full response. Force that
			// response through beginSequentialResponse so stale tail bytes are
			// truncated before any replacement is accepted.
			fetcher.connections = nil
			fetcher.rangeReprobeEligible = false
			fetcher.rangeValidatorPinned = false
			fetcher.sequentialRestartPending = true
		}
		return fetcher
	}
}

func (fm *FetcherManager) Close() error {
	return nil
}
