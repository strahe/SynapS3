package backend_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
)

// --- GetBucketAcl ---

func TestGetBucketAcl_ExistingBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "acl-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	data, err := tb.backend.GetBucketAcl(ctx, &s3.GetBucketAclInput{
		Bucket: aws.String("acl-bucket"),
	})
	if err != nil {
		t.Fatalf("GetBucketAcl: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil ACL data, got %v", data)
	}
}

func TestCreateBucketStoresDefaultACL(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedS3Account(t, tb, "user-access")
	acl := auth.ACL{
		Owner: "user-access",
		Grantees: []auth.Grantee{{
			Permission: auth.PermissionFullControl,
			Access:     "user-access",
			Type:       types.TypeCanonicalUser,
		}},
	}
	data, err := json.Marshal(acl)
	if err != nil {
		t.Fatalf("Marshal ACL: %v", err)
	}

	err = tb.backend.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("owned-bucket")}, data)
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	gotData, err := tb.backend.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String("owned-bucket")})
	if err != nil {
		t.Fatalf("GetBucketAcl: %v", err)
	}
	var got auth.ACL
	if err := json.Unmarshal(gotData, &got); err != nil {
		t.Fatalf("Unmarshal stored ACL: %v", err)
	}
	if got.Owner != "user-access" {
		t.Fatalf("owner = %q, want user-access", got.Owner)
	}
}

func TestGetBucketAcl_NonExistentBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	_, err := tb.backend.GetBucketAcl(ctx, &s3.GetBucketAclInput{
		Bucket: aws.String("no-such-bucket"),
	})
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}

	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrNoSuchBucket)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestBucketACLOperationsRejectMissingBucketInput(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	want := s3err.GetAPIError(s3err.ErrInvalidBucketName)

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "GetBucketAcl nil input",
			run: func() error {
				_, err := tb.backend.GetBucketAcl(ctx, nil)
				return err
			},
		},
		{
			name: "GetBucketAcl nil bucket",
			run: func() error {
				_, err := tb.backend.GetBucketAcl(ctx, &s3.GetBucketAclInput{})
				return err
			},
		},
		{
			name: "GetObjectAcl nil input",
			run: func() error {
				_, err := tb.backend.GetObjectAcl(ctx, nil)
				return err
			},
		},
		{
			name: "GetObjectAcl nil bucket",
			run: func() error {
				_, err := tb.backend.GetObjectAcl(ctx, &s3.GetObjectAclInput{})
				return err
			},
		},
		{
			name: "PutObjectAcl nil input",
			run: func() error {
				return tb.backend.PutObjectAcl(ctx, nil)
			},
		},
		{
			name: "PutObjectAcl nil bucket",
			run: func() error {
				return tb.backend.PutObjectAcl(ctx, &s3.PutObjectAclInput{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireAPIErrorCode(t, tt.run(), want)
		})
	}
}

// --- PutBucketAcl ---

func TestPutBucketAcl_WritableBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "put-acl-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	if err := tb.backend.PutBucketAcl(ctx, "put-acl-bucket", nil); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
}

func TestPutBucketAclPersistsACL(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedS3Account(t, tb, "new-owner")

	bkt := &model.Bucket{Name: "persist-acl-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}
	acl := auth.ACL{Owner: "new-owner"}
	data, err := json.Marshal(acl)
	if err != nil {
		t.Fatalf("Marshal ACL: %v", err)
	}

	if err := tb.backend.PutBucketAcl(ctx, "persist-acl-bucket", data); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	gotData, err := tb.backend.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String("persist-acl-bucket")})
	if err != nil {
		t.Fatalf("GetBucketAcl: %v", err)
	}
	var got auth.ACL
	if err := json.Unmarshal(gotData, &got); err != nil {
		t.Fatalf("Unmarshal stored ACL: %v", err)
	}
	if got.Owner != "new-owner" {
		t.Fatalf("owner = %q, want new-owner", got.Owner)
	}
	updated, err := tb.repos.Buckets.GetByName(ctx, "persist-acl-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated.OwnerAccessKey == nil || *updated.OwnerAccessKey != "new-owner" {
		t.Fatalf("owner_access_key = %v, want new-owner", updated.OwnerAccessKey)
	}
}

func TestPutBucketAclPreservesCurrentOwnerWhenACLHasNoOwner(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedS3Account(t, tb, "current-owner")

	currentOwner := "current-owner"
	acl, err := json.Marshal(auth.ACL{Owner: currentOwner})
	if err != nil {
		t.Fatalf("Marshal current ACL: %v", err)
	}
	bkt := &model.Bucket{Name: "preserve-owner-acl-bucket", Status: model.BucketStatusActive, OwnerAccessKey: &currentOwner, ACL: acl}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}
	nextACL, err := json.Marshal(auth.ACL{})
	if err != nil {
		t.Fatalf("Marshal next ACL: %v", err)
	}

	if err := tb.backend.PutBucketAcl(ctx, bkt.Name, nextACL); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
	updated, err := tb.repos.Buckets.GetByName(ctx, bkt.Name)
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated.OwnerAccessKey == nil || *updated.OwnerAccessKey != currentOwner {
		t.Fatalf("owner_access_key = %v, want current owner", updated.OwnerAccessKey)
	}
	got, err := auth.ParseACL(updated.ACL)
	if err != nil {
		t.Fatalf("ParseACL: %v", err)
	}
	if got.Owner != currentOwner {
		t.Fatalf("ACL owner = %q, want current owner", got.Owner)
	}
	if len(got.Grantees) != 1 ||
		got.Grantees[0].Access != currentOwner ||
		got.Grantees[0].Permission != auth.PermissionFullControl {
		t.Fatalf("ACL grantees = %#v, want explicit owner FULL_CONTROL grant", got.Grantees)
	}
}

func TestPutBucketAclRejectsUnknownOwner(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	bkt := &model.Bucket{Name: "unknown-put-owner-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}
	data, err := json.Marshal(auth.ACL{Owner: "missing-owner"})
	if err != nil {
		t.Fatalf("Marshal ACL: %v", err)
	}

	err = tb.backend.PutBucketAcl(ctx, bkt.Name, data)
	if err == nil {
		t.Fatal("expected PutBucketAcl to reject unknown owner")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError, got %T: %v", err, err)
	}
	if want := s3err.GetAPIError(s3err.ErrAccessDenied); apiErr.Code != want.Code {
		t.Fatalf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestChangeBucketOwnerUpdatesACL(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedS3Account(t, tb, "replacement-owner")

	bkt := &model.Bucket{Name: "change-owner-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	if err := tb.backend.ChangeBucketOwner(ctx, "change-owner-bucket", "replacement-owner"); err != nil {
		t.Fatalf("ChangeBucketOwner: %v", err)
	}
	gotData, err := tb.backend.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String("change-owner-bucket")})
	if err != nil {
		t.Fatalf("GetBucketAcl: %v", err)
	}
	var got auth.ACL
	if err := json.Unmarshal(gotData, &got); err != nil {
		t.Fatalf("Unmarshal stored ACL: %v", err)
	}
	if got.Owner != "replacement-owner" {
		t.Fatalf("owner = %q, want replacement-owner", got.Owner)
	}
}

func TestListBucketsAndOwnersReturnsStoredOwners(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedS3Account(t, tb, "owner-a")
	seedS3Account(t, tb, "owner-b")

	for _, seed := range []struct {
		name  string
		owner string
		acl   []byte
	}{
		{name: "owner-list-a", owner: "owner-a"},
		{name: "owner-list-b", owner: "owner-b"},
		{name: "owner-list-unassigned"},
		{name: "owner-list-malformed", acl: []byte("{")},
	} {
		bkt := &model.Bucket{Name: seed.name, Status: model.BucketStatusActive}
		switch {
		case seed.acl != nil:
			bkt.ACL = seed.acl
		case seed.owner != "":
			data, err := json.Marshal(auth.ACL{Owner: seed.owner})
			if err != nil {
				t.Fatalf("Marshal ACL: %v", err)
			}
			bkt.ACL = data
			bkt.OwnerAccessKey = &seed.owner
		}
		if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
			t.Fatalf("Buckets.Create(%s): %v", seed.name, err)
		}
	}

	got, err := tb.backend.ListBucketsAndOwners(ctx)
	if err != nil {
		t.Fatalf("ListBucketsAndOwners: %v", err)
	}

	want := map[string]string{
		"owner-list-a":          "owner-a",
		"owner-list-b":          "owner-b",
		"owner-list-malformed":  "",
		"owner-list-unassigned": "",
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d: %#v", len(got), len(want), got)
	}
	for _, bucket := range got {
		if owner, ok := want[bucket.Name]; !ok || bucket.Owner != owner {
			t.Fatalf("bucket = %#v, want owner %q (known=%v)", bucket, owner, ok)
		}
	}
}

func TestPutBucketOwnershipControlsRejectsUnsupportedModes(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedS3Account(t, tb, "owner-before")
	owner := "owner-before"
	acl, err := json.Marshal(auth.ACL{Owner: owner})
	if err != nil {
		t.Fatalf("Marshal ACL: %v", err)
	}
	bkt := &model.Bucket{Name: "ownership-controls-bucket", Status: model.BucketStatusActive, OwnerAccessKey: &owner, ACL: acl}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	if err := tb.backend.PutBucketOwnershipControls(ctx, bkt.Name, types.ObjectOwnershipBucketOwnerPreferred); err != nil {
		t.Fatalf("PutBucketOwnershipControls(%s): %v", types.ObjectOwnershipBucketOwnerPreferred, err)
	}
	updated, err := tb.repos.Buckets.GetByName(ctx, bkt.Name)
	if err != nil {
		t.Fatalf("GetByName after PutBucketOwnershipControls(%s): %v", types.ObjectOwnershipBucketOwnerPreferred, err)
	}
	if string(updated.ACL) != string(acl) {
		t.Fatalf("ACL changed after PutBucketOwnershipControls(%s): got %s want %s", types.ObjectOwnershipBucketOwnerPreferred, updated.ACL, acl)
	}
	if updated.OwnerAccessKey == nil || *updated.OwnerAccessKey != owner {
		t.Fatalf("owner changed after PutBucketOwnershipControls(%s): got %v want %s", types.ObjectOwnershipBucketOwnerPreferred, updated.OwnerAccessKey, owner)
	}

	for _, ownership := range []types.ObjectOwnership{
		types.ObjectOwnershipBucketOwnerEnforced,
		types.ObjectOwnershipObjectWriter,
	} {
		err := tb.backend.PutBucketOwnershipControls(ctx, bkt.Name, ownership)
		if err == nil {
			t.Fatalf("expected PutBucketOwnershipControls(%s) to fail", ownership)
		}
		s3Err, ok := err.(s3err.S3Error)
		if !ok {
			t.Fatalf("expected s3err.S3Error for %s, got %T: %v", ownership, err, err)
		}
		apiErr := s3Err.BaseError()
		if want := (s3err.InvalidArgumentError{}).BaseError(); apiErr.Code != want.Code {
			t.Fatalf("error code for %s = %q, want %q", ownership, apiErr.Code, want.Code)
		}
		updated, err := tb.repos.Buckets.GetByName(ctx, bkt.Name)
		if err != nil {
			t.Fatalf("GetByName after PutBucketOwnershipControls(%s): %v", ownership, err)
		}
		if string(updated.ACL) != string(acl) {
			t.Fatalf("ACL changed after PutBucketOwnershipControls(%s): got %s want %s", ownership, updated.ACL, acl)
		}
		if updated.OwnerAccessKey == nil || *updated.OwnerAccessKey != owner {
			t.Fatalf("owner changed after PutBucketOwnershipControls(%s): got %v want %s", ownership, updated.OwnerAccessKey, owner)
		}
	}

	err = tb.backend.PutBucketOwnershipControls(ctx, "missing-ownership-bucket", types.ObjectOwnershipBucketOwnerPreferred)
	if err == nil {
		t.Fatal("expected missing bucket error")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError, got %T: %v", err, err)
	}
	if want := s3err.GetAPIError(s3err.ErrNoSuchBucket); apiErr.Code != want.Code {
		t.Fatalf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestDeleteBucketOwnershipControlsIsCompatibilityNoop(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedS3Account(t, tb, "owner-before")
	owner := "owner-before"
	acl, err := json.Marshal(auth.ACL{Owner: owner})
	if err != nil {
		t.Fatalf("Marshal ACL: %v", err)
	}
	bkt := &model.Bucket{Name: "delete-ownership-controls-bucket", Status: model.BucketStatusActive, OwnerAccessKey: &owner, ACL: acl}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}

	if err := tb.backend.DeleteBucketOwnershipControls(ctx, bkt.Name); err != nil {
		t.Fatalf("DeleteBucketOwnershipControls: %v", err)
	}
	updated, err := tb.repos.Buckets.GetByName(ctx, bkt.Name)
	if err != nil {
		t.Fatalf("GetByName after DeleteBucketOwnershipControls: %v", err)
	}
	if string(updated.ACL) != string(acl) {
		t.Fatalf("ACL changed after DeleteBucketOwnershipControls: got %s want %s", updated.ACL, acl)
	}
	if updated.OwnerAccessKey == nil || *updated.OwnerAccessKey != owner {
		t.Fatalf("owner changed after DeleteBucketOwnershipControls: got %v want %s", updated.OwnerAccessKey, owner)
	}

	err = tb.backend.DeleteBucketOwnershipControls(ctx, "missing-delete-ownership-bucket")
	if err == nil {
		t.Fatal("expected missing bucket error")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError, got %T: %v", err, err)
	}
	if want := s3err.GetAPIError(s3err.ErrNoSuchBucket); apiErr.Code != want.Code {
		t.Fatalf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestGetBucketPolicyMissingPolicyReturnsNoSuchBucketPolicy(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "policy-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	_, err := tb.backend.GetBucketPolicy(ctx, "policy-bucket")
	if err == nil {
		t.Fatal("expected missing bucket policy error")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
	if apiErr.Code != want.Code {
		t.Fatalf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestGetBucketPolicyMissingBucketReturnsNoSuchBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	_, err := tb.backend.GetBucketPolicy(ctx, "missing-policy-bucket")
	if err == nil {
		t.Fatal("expected missing bucket error")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrNoSuchBucket)
	if apiErr.Code != want.Code {
		t.Fatalf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

// --- GetBucketTagging ---

func TestGetBucketTagging_ReturnsAPIError(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "tag-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	_, err := tb.backend.GetBucketTagging(ctx, "tag-bucket")
	if err == nil {
		t.Fatal("expected error from GetBucketTagging")
	}

	// Must be a raw APIError (not wrapped), so VersityGW type assertion works.
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError (unwrapped), got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrBucketTaggingNotFound)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestGetBucketTagging_NonExistentBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	_, err := tb.backend.GetBucketTagging(ctx, "no-such-tag-bucket")
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

// --- GetBucketVersioning ---

func TestGetBucketVersioning_ExistingBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "ver-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	out, err := tb.backend.GetBucketVersioning(ctx, "ver-bucket")
	if err != nil {
		t.Fatalf("GetBucketVersioning: %v", err)
	}
	if out.Status == nil || *out.Status != types.BucketVersioningStatusEnabled {
		t.Errorf("Status = %v, want Enabled", out.Status)
	}
	if out.MFADelete != nil {
		t.Errorf("MFADelete = %v, want nil", out.MFADelete)
	}
}

func TestPutBucketVersioningEnabledIsNoop(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "put-ver-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	if err := tb.backend.PutBucketVersioning(ctx, "put-ver-bucket", types.BucketVersioningStatusEnabled); err != nil {
		t.Fatalf("PutBucketVersioning(Enabled): %v", err)
	}
}

func TestPutBucketVersioningSuspendedRejected(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "put-ver-suspended-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	err := tb.backend.PutBucketVersioning(ctx, "put-ver-suspended-bucket", types.BucketVersioningStatusSuspended)
	if err == nil {
		t.Fatal("expected error")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if apiErr.Code != "InvalidBucketState" {
		t.Fatalf("error code = %q, want InvalidBucketState", apiErr.Code)
	}
}

func TestGetBucketVersioning_NonExistentBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	_, err := tb.backend.GetBucketVersioning(ctx, "ghost-bucket")
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError, got %T: %v", err, err)
	}
	if apiErr.Code != "NoSuchBucket" {
		t.Errorf("error code = %q, want NoSuchBucket", apiErr.Code)
	}
}

// --- GetObjectLockConfiguration ---

func TestGetObjectLockConfiguration_ReturnsNotFound(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "lock-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	_, err := tb.backend.GetObjectLockConfiguration(ctx, "lock-bucket")
	if err == nil {
		t.Fatal("expected error from GetObjectLockConfiguration")
	}

	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

// --- GetBucketOwnershipControls ---

func TestGetBucketOwnershipControls_ReturnsAclCompatibleOwnership(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "own-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	ownership, err := tb.backend.GetBucketOwnershipControls(ctx, "own-bucket")
	if err != nil {
		t.Fatalf("GetBucketOwnershipControls: %v", err)
	}
	if ownership != types.ObjectOwnershipBucketOwnerPreferred {
		t.Errorf("ownership = %q, want %q", ownership, types.ObjectOwnershipBucketOwnerPreferred)
	}
}

// --- GetObjectAcl ---

func TestGetObjectAcl_VisibleBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "obj-acl-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	out, err := tb.backend.GetObjectAcl(ctx, &s3.GetObjectAclInput{
		Bucket: aws.String("obj-acl-bucket"),
	})
	if err != nil {
		t.Fatalf("GetObjectAcl: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
}

// --- PutObjectAcl ---

func TestPutObjectAcl_WritableBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "put-obj-acl", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	err := tb.backend.PutObjectAcl(ctx, &s3.PutObjectAclInput{
		Bucket: aws.String("put-obj-acl"),
	})
	if err != nil {
		t.Fatalf("PutObjectAcl: %v", err)
	}
}
