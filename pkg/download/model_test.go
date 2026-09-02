package download

import (
	"encoding/json"
	"testing"

	"github.com/GopeedLab/gopeed/internal/controller"
	"github.com/GopeedLab/gopeed/internal/fetcher"
	"github.com/GopeedLab/gopeed/pkg/base"
)

type snapshotTestFetcher struct {
	live     *fetcher.FetcherMeta
	snapshot *fetcher.FetcherMeta
}

func (f *snapshotTestFetcher) Setup(*controller.Controller)               {}
func (f *snapshotTestFetcher) Resolve(*base.Request, *base.Options) error { return nil }
func (f *snapshotTestFetcher) Start() error                               { return nil }
func (f *snapshotTestFetcher) Patch(*base.Request, *base.Options) error   { return nil }
func (f *snapshotTestFetcher) Pause() error                               { return nil }
func (f *snapshotTestFetcher) Close() error                               { return nil }
func (f *snapshotTestFetcher) Stats() any                                 { return nil }
func (f *snapshotTestFetcher) Meta() *fetcher.FetcherMeta                 { return f.live }
func (f *snapshotTestFetcher) MetaSnapshot() *fetcher.FetcherMeta         { return f.snapshot }
func (f *snapshotTestFetcher) Progress() fetcher.Progress                 { return nil }
func (f *snapshotTestFetcher) Wait() error                                { return nil }

func TestCalcSpeedResetOnRollback(t *testing.T) {
	speedArr := []int64{1024, 2048, 4096}

	if got := calcSpeed(&speedArr, -512, 1); got != 0 {
		t.Fatalf("calcSpeed() = %d, want 0 after rollback", got)
	}
	if len(speedArr) != 0 {
		t.Fatalf("speed window len = %d, want 0 after rollback", len(speedArr))
	}

	if got := calcSpeed(&speedArr, 1024, 1); got != 1024 {
		t.Fatalf("calcSpeed() = %d, want 1024 after reset", got)
	}
}

func TestTaskMarshalJSONUsesFetcherMetaSnapshot(t *testing.T) {
	live := &fetcher.FetcherMeta{
		Req:  &base.Request{URL: "https://example.invalid/live"},
		Res:  &base.Resource{Size: 1, Files: []*base.FileInfo{{Name: "live.data", Size: 2}}},
		Opts: &base.Options{},
	}
	snapshot := &fetcher.FetcherMeta{
		Req:  &base.Request{URL: "https://example.invalid/snapshot"},
		Res:  &base.Resource{Size: 3, Files: []*base.FileInfo{{Name: "snapshot.data", Size: 3}}},
		Opts: &base.Options{},
	}
	task := &Task{
		ID:      "snapshot-task",
		Meta:    live,
		fetcher: &snapshotTestFetcher{live: live, snapshot: snapshot},
	}

	encoded, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Meta *fetcher.FetcherMeta `json:"meta"`
		Name string               `json:"name"`
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got.Meta.Res.Size != 3 || got.Meta.Res.Files[0].Size != 3 {
		t.Fatalf("serialized metadata = %d/%d, want snapshot 3/3", got.Meta.Res.Size, got.Meta.Res.Files[0].Size)
	}
	if got.Name != "snapshot.data" {
		t.Fatalf("serialized name = %q, want snapshot.data", got.Name)
	}
}
