package admin

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/types"
)

type taskSubjectInfo struct {
	SubjectType    string               `json:"subject_type"`
	SubjectKey     string               `json:"subject_key"`
	Bucket         string               `json:"bucket,omitempty"`
	File           *taskSubjectFile     `json:"file,omitempty"`
	Size           *int64               `json:"size,omitempty"`
	CopyIndex      *int                 `json:"copy_index,omitempty"`
	LocalDataSetID *int64               `json:"local_data_set_id,omitempty"`
	DataSetID      *types.OnChainID     `json:"data_set_id,omitempty"`
	Provider       *taskSubjectProvider `json:"provider,omitempty"`
	SourceProvider *taskSubjectProvider `json:"source_provider,omitempty"`
	TargetProvider *taskSubjectProvider `json:"target_provider,omitempty"`
	Wallet         *taskSubjectWallet   `json:"wallet,omitempty"`
	ContentCount   *int64               `json:"content_count,omitempty"`
}

type taskSubjectFile struct {
	Key           string `json:"key"`
	Source        string `json:"source"`
	OtherVersions int64  `json:"other_versions"`
}

type taskSubjectProvider struct {
	ID         string `json:"id"`
	Name       string `json:"name,omitempty"`
	ServiceURL string `json:"service_url,omitempty"`
}

type taskSubjectWallet struct {
	Operation string `json:"operation"`
	Amount    string `json:"amount,omitempty"`
}

func (s *Server) handleAPITaskSubject(w http.ResponseWriter, r *http.Request) {
	info, err := s.loadTaskSubject(r.Context(), r.PathValue("subject_type"), r.PathValue("subject_key"))
	switch {
	case errors.Is(err, repository.ErrInvalidInput):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid task subject"})
	case errors.Is(err, repository.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "subject information unavailable"})
	case err != nil:
		s.logger.Error("api: failed to read task subject", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
	default:
		writeJSON(w, http.StatusOK, info)
	}
}

func (s *Server) loadTaskSubject(ctx context.Context, subjectType, key string) (*taskSubjectInfo, error) {
	info := &taskSubjectInfo{SubjectType: subjectType, SubjectKey: key}
	if subjectType == "provider" {
		id, err := types.ParseOnChainID("provider_id", key)
		if err != nil {
			return nil, repository.ErrInvalidInput
		}
		row, err := s.repos.Observability.GetProviderSubject(ctx, id)
		if err != nil {
			return nil, err
		}
		info.Provider = &taskSubjectProvider{ID: row.ProviderID.String(), Name: row.Name, ServiceURL: row.ServiceURL}
		return info, nil
	}

	var id int64
	switch subjectType {
	case "storage_commit_request":
		if key == "" {
			return nil, repository.ErrInvalidInput
		}
	case "storage_content", "storage_copy", "storage_data_set", "bucket", "storage_replacement", "wallet_operation":
		var err error
		id, err = strconv.ParseInt(key, 10, 64)
		if err != nil || id <= 0 {
			return nil, repository.ErrInvalidInput
		}
	default:
		return nil, repository.ErrInvalidInput
	}

	var bucketID int64
	var storage *repository.StorageSubject
	var err error
	switch subjectType {
	case "bucket":
		bucketID = id
	case "wallet_operation":
		row, err := s.repos.WalletOperations.GetOperationSubject(ctx, id)
		if err != nil {
			return nil, err
		}
		info.Wallet = &taskSubjectWallet{Operation: string(row.Type), Amount: row.Amount}
	case "storage_replacement":
		row, err := s.repos.Replacements.GetReplacementSubject(ctx, id)
		if err != nil {
			return nil, err
		}
		bucketID, info.CopyIndex = row.BucketID, &row.CopyIndex
		info.SourceProvider, err = s.taskSubjectProvider(ctx, row.SourceProviderID)
		if err != nil {
			return nil, err
		}
		info.TargetProvider, err = s.taskSubjectProvider(ctx, row.TargetProviderID)
		if err != nil {
			return nil, err
		}
	case "storage_content":
		storage, err = s.repos.Contents.GetContentSubject(ctx, id)
		if errors.Is(err, repository.ErrNotFound) {
			storage, err = &repository.StorageSubject{ContentID: id}, nil
		}
	case "storage_copy":
		storage, err = s.repos.Contents.GetCopySubject(ctx, id)
	case "storage_data_set":
		storage, err = s.repos.Contents.GetDataSetSubject(ctx, id)
	case "storage_commit_request":
		storage, err = s.repos.Contents.GetCommitSubject(ctx, key)
	}
	if err != nil {
		return nil, err
	}
	if storage != nil {
		bucketID = storage.BucketID
		info.Size, info.CopyIndex = storage.Size, storage.CopyIndex
		info.LocalDataSetID, info.DataSetID, info.ContentCount = storage.LocalDataSetID, storage.DataSetID, storage.ContentCount
		if storage.ProviderID != nil {
			info.Provider, err = s.taskSubjectProvider(ctx, *storage.ProviderID)
			if err != nil {
				return nil, err
			}
		}
		if storage.ContentID != 0 {
			file, err := s.repos.Objects.GetContentFileSample(ctx, storage.ContentID)
			if err != nil && (!errors.Is(err, repository.ErrNotFound) || bucketID == 0) {
				return nil, err
			}
			if err == nil {
				info.File = &taskSubjectFile{Key: file.Key, Source: file.Source, OtherVersions: file.OtherVersions}
				bucketID, info.Size = file.BucketID, &file.Size
			}
		}
	}
	if bucketID != 0 {
		names, err := s.repos.Buckets.GetNamesByIDs(ctx, []int64{bucketID})
		if err != nil {
			return nil, err
		}
		info.Bucket = names[bucketID]
		if subjectType == "bucket" && info.Bucket == "" {
			return nil, repository.ErrNotFound
		}
	}
	return info, nil
}

func (s *Server) taskSubjectProvider(ctx context.Context, id types.OnChainID) (*taskSubjectProvider, error) {
	info := &taskSubjectProvider{ID: id.String()}
	row, err := s.repos.Observability.GetProviderSubject(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		return info, nil
	}
	if err != nil {
		return nil, err
	}
	info.Name, info.ServiceURL = row.Name, row.ServiceURL
	return info, nil
}
