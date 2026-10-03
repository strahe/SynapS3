package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/types"
)

type subjectStorageRepo struct {
	repository.StorageContentRepository
	err error
}

func (r subjectStorageRepo) GetContentSubject(_ context.Context, id int64) (*repository.StorageSubject, error) {
	return &repository.StorageSubject{BucketID: 1, ContentID: id, Size: new(int64(42))}, r.err
}

func (r subjectStorageRepo) GetCopySubject(_ context.Context, id int64) (*repository.StorageSubject, error) {
	return &repository.StorageSubject{BucketID: 1, ContentID: id + 1, CopyIndex: new(0), LocalDataSetID: new(int64(23)), ProviderID: new(types.NewOnChainID(701)), DataSetID: new(types.NewOnChainID(801))}, r.err
}

func (r subjectStorageRepo) GetDataSetSubject(_ context.Context, id int64) (*repository.StorageSubject, error) {
	return &repository.StorageSubject{BucketID: 1, CopyIndex: new(1), LocalDataSetID: new(id), ProviderID: new(types.NewOnChainID(703)), DataSetID: new(types.NewOnChainID(802))}, r.err
}

func (r subjectStorageRepo) GetCommitSubject(_ context.Context, requestID string) (*repository.StorageSubject, error) {
	if requestID != "registration:example" {
		return nil, repository.ErrNotFound
	}
	return &repository.StorageSubject{BucketID: 1, CopyIndex: new(2), LocalDataSetID: new(int64(25)), ProviderID: new(types.NewOnChainID(704)), DataSetID: new(types.NewOnChainID(803)), ContentCount: new(int64(2))}, r.err
}

type subjectObjectRepo struct {
	repository.ObjectRepository
	err error
}

func (r subjectObjectRepo) GetContentFileSample(_ context.Context, id int64) (*repository.ContentFileSample, error) {
	return &repository.ContentFileSample{BucketID: 1, Key: fmt.Sprintf("complete/path/content-%d.txt", id), Size: 42, Source: "deleted", OtherVersions: 2}, r.err
}

type subjectBucketRepo struct{ repository.BucketRepository }

func (subjectBucketRepo) GetNamesByIDs(_ context.Context, ids []int64) (map[int64]string, error) {
	return map[int64]string{1: "files"}, nil
}

type subjectProviderRepo struct {
	repository.ObservabilityRepository
	targetErr error
}

func (r subjectProviderRepo) GetProviderSubject(_ context.Context, id types.OnChainID) (*repository.ProviderSubject, error) {
	if id.String() == "702" {
		if r.targetErr != nil {
			return nil, r.targetErr
		}
		return nil, repository.ErrNotFound
	}
	return &repository.ProviderSubject{ProviderID: id, Name: "North", ServiceURL: "https://provider.example"}, nil
}

type subjectReplacementRepo struct {
	repository.StorageReplacementRepository
}

func (subjectReplacementRepo) GetReplacementSubject(context.Context, int64) (*repository.ReplacementSubject, error) {
	return &repository.ReplacementSubject{BucketID: 1, CopyIndex: 0, SourceProviderID: types.NewOnChainID(701), TargetProviderID: types.NewOnChainID(702)}, nil
}

func TestAPITaskSubjects(t *testing.T) {
	f := newAdminTaskFixture(t)
	f.repos.Contents = subjectStorageRepo{StorageContentRepository: f.repos.Contents}
	f.repos.Objects = subjectObjectRepo{ObjectRepository: f.repos.Objects}
	f.repos.Buckets = subjectBucketRepo{f.repos.Buckets}
	f.repos.Observability = subjectProviderRepo{ObservabilityRepository: f.repos.Observability}
	f.repos.Replacements = subjectReplacementRepo{f.repos.Replacements}
	op, _, err := f.repos.WalletOperations.CreateOrGet(t.Context(), repository.CreateWalletOperationInput{Type: model.WalletOperationTypeFund, ClientRequestID: "subject-fund", Amount: "123456789012345678901"})
	if err != nil || op.ID != 1 {
		t.Fatalf("create wallet operation: %#v, %v", op, err)
	}
	cases := []struct {
		typeName string
		key      string
		check    func(*taskSubjectInfo) bool
	}{
		{"storage_content", "128", func(info *taskSubjectInfo) bool {
			return info.File != nil && info.File.Key == "complete/path/content-128.txt" && info.File.Source == "deleted" && info.File.OtherVersions == 2 && info.Size != nil && *info.Size == 42 && info.CopyIndex == nil
		}},
		{"storage_copy", "447", func(info *taskSubjectInfo) bool {
			return info.File != nil && info.File.Key == "complete/path/content-448.txt" && info.CopyIndex != nil && *info.CopyIndex == 0 && info.Provider != nil && info.Provider.ID == "701" && info.LocalDataSetID != nil && *info.LocalDataSetID == 23
		}},
		{"storage_data_set", "23", func(info *taskSubjectInfo) bool {
			return info.LocalDataSetID != nil && *info.LocalDataSetID == 23 && info.CopyIndex != nil && *info.CopyIndex == 1 && info.Provider != nil && info.Provider.ID == "703" && info.DataSetID != nil && info.DataSetID.String() == "802"
		}},
		{"bucket", "1", func(info *taskSubjectInfo) bool { return info.Bucket == "files" }},
		{"provider", "184467440737095516160", func(info *taskSubjectInfo) bool {
			return info.Provider.ID == "184467440737095516160" && info.Provider.ServiceURL == "https://provider.example"
		}},
		{"storage_replacement", "1", func(info *taskSubjectInfo) bool {
			return info.SourceProvider.Name == "North" && info.TargetProvider.ID == "702" && info.TargetProvider.Name == ""
		}},
		{"wallet_operation", "1", func(info *taskSubjectInfo) bool {
			return info.Wallet.Operation == "fund" && info.Wallet.Amount == "123456789012345678901"
		}},
		{"storage_commit_request", "registration:example", func(info *taskSubjectInfo) bool {
			return info.ContentCount != nil && *info.ContentCount == 2 && info.LocalDataSetID != nil && *info.LocalDataSetID == 25 && info.CopyIndex != nil && *info.CopyIndex == 2 && info.Provider != nil && info.Provider.ID == "704" && info.DataSetID != nil && info.DataSetID.String() == "803"
		}},
	}
	for _, c := range cases {
		t.Run(c.typeName, func(t *testing.T) {
			rr := f.request(http.MethodGet, "/api/v1/task-subjects/"+c.typeName+"/"+c.key, nil)
			var info taskSubjectInfo
			decodeJSON(t, rr, &info)
			if rr.Code != http.StatusOK || info.SubjectType != c.typeName || info.SubjectKey != c.key || !c.check(&info) {
				t.Fatalf("subject = %s, status=%d", rr.Body.String(), rr.Code)
			}
		})
	}
	for _, path := range []string{"storage_copy/0", "storage_copy/-1", "bucket/abc", "provider/invalid", "provider/115792089237316195423570985008687907853269984665640564039457584007913129639936", "system/cache-capacity", "unknown/1"} {
		if rr := f.request(http.MethodGet, "/api/v1/task-subjects/"+path, nil); rr.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d", path, rr.Code)
		}
	}
	for _, path := range []string{"bucket/2", "wallet_operation/2", "provider/702"} {
		if rr := f.request(http.MethodGet, "/api/v1/task-subjects/"+path, nil); rr.Code != http.StatusNotFound {
			t.Fatalf("%s status=%d", path, rr.Code)
		}
	}
	for _, c := range []struct {
		err    error
		status int
	}{{repository.ErrNotFound, http.StatusNotFound}, {errors.New("database unavailable"), http.StatusInternalServerError}} {
		f.repos.Contents = subjectStorageRepo{err: c.err}
		if rr := f.request(http.MethodGet, "/api/v1/task-subjects/storage_copy/1", nil); rr.Code != c.status {
			t.Fatalf("error %v status=%d", c.err, rr.Code)
		}
	}
	f.repos.Contents = subjectStorageRepo{err: repository.ErrNotFound}
	if rr := f.request(http.MethodGet, "/api/v1/task-subjects/storage_content/1", nil); rr.Code != http.StatusOK {
		t.Fatalf("deleted content status=%d body=%s", rr.Code, rr.Body.String())
	}
	f.repos.Objects = subjectObjectRepo{err: repository.ErrNotFound}
	if rr := f.request(http.MethodGet, "/api/v1/task-subjects/storage_content/1", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("missing content history status=%d", rr.Code)
	}
	f.repos.Contents = subjectStorageRepo{}
	rr := f.request(http.MethodGet, "/api/v1/task-subjects/storage_copy/1", nil)
	var partial taskSubjectInfo
	decodeJSON(t, rr, &partial)
	if rr.Code != http.StatusOK || partial.File != nil || partial.CopyIndex == nil || partial.Provider == nil {
		t.Fatalf("partial copy status=%d body=%s", rr.Code, rr.Body.String())
	}
	f.repos.Observability = subjectProviderRepo{targetErr: errors.New("database unavailable")}
	if rr := f.request(http.MethodGet, "/api/v1/task-subjects/storage_replacement/1", nil); rr.Code != http.StatusInternalServerError {
		t.Fatalf("target provider error status=%d", rr.Code)
	}
}
