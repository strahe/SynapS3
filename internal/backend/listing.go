package backend

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

const listingBatchSize = 1000

func (b *SynapseBackend) listCurrentObjects(ctx context.Context, bucketID int64, prefix, delimiter, afterKey string, maxKeys int) ([]model.Object, []types.CommonPrefix, bool, string, error) {
	if maxKeys <= 0 {
		return nil, nil, false, "", nil
	}

	var objects []model.Object
	var prefixes []types.CommonPrefix
	seenPrefixes := make(map[string]struct{})
	cursor := afterKey
	lastMarker := afterKey

	for {
		rows, err := b.repos.Objects.ListByBucket(ctx, bucketID, prefix, cursor, listingBatchSize)
		if err != nil {
			return nil, nil, false, "", err
		}
		if len(rows) == 0 {
			return objects, prefixes, false, "", nil
		}

		for _, obj := range rows {
			cursor = obj.Key
			if commonPrefix, ok := listingCommonPrefix(obj.Key, prefix, delimiter); ok {
				if afterKey != "" && commonPrefix <= afterKey {
					lastMarker = obj.Key
					continue
				}
				if _, exists := seenPrefixes[commonPrefix]; exists {
					lastMarker = obj.Key
					continue
				}
				if len(objects)+len(prefixes) >= maxKeys {
					return objects, prefixes, true, lastMarker, nil
				}
				seenPrefixes[commonPrefix] = struct{}{}
				cp := commonPrefix
				prefixes = append(prefixes, types.CommonPrefix{Prefix: &cp})
				lastMarker = obj.Key
				continue
			}

			if len(objects)+len(prefixes) >= maxKeys {
				return objects, prefixes, true, lastMarker, nil
			}
			objects = append(objects, obj)
			lastMarker = obj.Key
		}

		if len(rows) < listingBatchSize {
			return objects, prefixes, false, "", nil
		}
	}
}

func (b *SynapseBackend) listVersions(ctx context.Context, bucketID int64, prefix, delimiter, keyMarker, versionIDMarker string, maxKeys int) ([]repository.ObjectVersionListItem, []types.CommonPrefix, bool, string, string, error) {
	if maxKeys <= 0 {
		return nil, nil, false, "", "", nil
	}

	var versions []repository.ObjectVersionListItem
	var prefixes []types.CommonPrefix
	seenPrefixes := make(map[string]struct{})
	cursorKey := keyMarker
	cursorVersion := versionIDMarker
	lastKeyMarker := keyMarker
	lastVersionMarker := versionIDMarker

	for {
		rows, err := b.repos.Objects.ListVersionsByBucket(ctx, bucketID, prefix, cursorKey, cursorVersion, listingBatchSize)
		if err != nil {
			return nil, nil, false, "", "", err
		}
		if len(rows) == 0 {
			return versions, prefixes, false, "", "", nil
		}

		for _, row := range rows {
			cursorKey = row.Key
			cursorVersion = row.VersionID
			if commonPrefix, ok := listingCommonPrefix(row.Key, prefix, delimiter); ok {
				if keyMarker != "" && commonPrefix <= keyMarker {
					lastKeyMarker = row.Key
					lastVersionMarker = row.VersionID
					continue
				}
				if _, exists := seenPrefixes[commonPrefix]; exists {
					lastKeyMarker = row.Key
					lastVersionMarker = row.VersionID
					continue
				}
				if len(versions)+len(prefixes) >= maxKeys {
					return versions, prefixes, true, lastKeyMarker, lastVersionMarker, nil
				}
				seenPrefixes[commonPrefix] = struct{}{}
				cp := commonPrefix
				prefixes = append(prefixes, types.CommonPrefix{Prefix: &cp})
				lastKeyMarker = row.Key
				lastVersionMarker = row.VersionID
				continue
			}

			if len(versions)+len(prefixes) >= maxKeys {
				return versions, prefixes, true, lastKeyMarker, lastVersionMarker, nil
			}
			versions = append(versions, row)
			lastKeyMarker = row.Key
			lastVersionMarker = row.VersionID
		}

		if len(rows) < listingBatchSize {
			return versions, prefixes, false, "", "", nil
		}
	}
}

func listingCommonPrefix(key, prefix, delimiter string) (string, bool) {
	if delimiter == "" {
		return "", false
	}
	suffix := strings.TrimPrefix(key, prefix)
	before, _, found := strings.Cut(suffix, delimiter)
	if !found {
		return "", false
	}
	return prefix + before + delimiter, true
}
