package storagecommit

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func TestSubmissionEnvelopeRoundTrip(t *testing.T) {
	submission := testCommitSubmission(t)

	encoded, err := EncodeSubmission(submission)
	if err != nil {
		t.Fatalf("EncodeSubmission: %v", err)
	}
	decoded, err := DecodeSubmission(encoded)
	if err != nil {
		t.Fatalf("DecodeSubmission: %v", err)
	}
	if decoded.Kind != submission.Kind || decoded.TransactionID != submission.TransactionID || decoded.StatusURL != submission.StatusURL {
		t.Fatalf("decoded submission = %#v, want %#v", decoded, submission)
	}
}

func TestSubmissionEnvelopeDecodesBeta1Fixture(t *testing.T) {
	const beta1Envelope = `{"version":1,"submission":{"kind":"add-pieces","transactionId":"0x0000000000000000000000000000000000000000000000000000000000000011","statusUrl":"https://provider.example/commit/0x0000000000000000000000000000000000000000000000000000000000000011","providerId":"1","identity":{"payer":"0x0000000000000000000000000000000000001234","chainId":314159,"recordKeeper":"0x0000000000000000000000000000000000005678"},"dataSet":{"providerId":"1","dataSetId":"42","clientDataSetId":"7"},"clientDataSetId":null,"pieceCids":[{"/":"bafkzcibcaaces3nobte6ezpp4wqan2age2s5yxcatzotcvobhgcmv5wi2xh5mbi"}]}}`

	decoded, err := DecodeSubmission(beta1Envelope)
	if err != nil {
		t.Fatalf("DecodeSubmission beta.1 fixture: %v", err)
	}
	if decoded.Kind != storage.CommitKindAddPieces || decoded.TransactionID != "0x0000000000000000000000000000000000000000000000000000000000000011" {
		t.Fatalf("decoded beta.1 submission = %#v", decoded)
	}
	if decoded.ProviderID.String() != "1" || decoded.Identity.ChainID != sdktypes.ChainID(314159) || decoded.Identity.Payer != common.HexToAddress("0x1234") {
		t.Fatalf("decoded beta.1 identity = provider %s, chain %d", decoded.ProviderID.String(), decoded.Identity.ChainID)
	}
	if decoded.DataSet == nil || decoded.DataSet.ProviderID().String() != "1" || decoded.DataSet.DataSetID().String() != "42" || decoded.DataSet.ClientDataSetID().String() != "7" {
		t.Fatalf("decoded beta.1 data set = %#v", decoded.DataSet)
	}
	if decoded.ClientDataSetID != nil || len(decoded.PieceCIDs) != 1 || decoded.PieceCIDs[0].String() != "bafkzcibcaaces3nobte6ezpp4wqan2age2s5yxcatzotcvobhgcmv5wi2xh5mbi" {
		t.Fatalf("decoded beta.1 recovery fields = %#v", decoded)
	}
}

func TestSubmissionEnvelopeRejectsUnsupportedOrMalformedData(t *testing.T) {
	encoded, err := EncodeSubmission(testCommitSubmission(t))
	if err != nil {
		t.Fatalf("EncodeSubmission: %v", err)
	}
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "unsupported version", value: strings.Replace(encoded, `"version":1`, `"version":2`, 1), want: "unsupported storage commit submission version 2"},
		{name: "unknown envelope field", value: strings.TrimSuffix(encoded, "}") + `,"extra":true}`, want: "unknown field"},
		{name: "trailing data", value: encoded + ` {}`, want: "trailing data"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeSubmission(test.value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("DecodeSubmission error = %v, want %q", err, test.want)
			}
		})
	}
}

func testCommitSubmission(t *testing.T) storage.CommitSubmission {
	t.Helper()
	mh, err := multihash.Sum([]byte("submission-envelope-test"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("create multihash: %v", err)
	}
	return storage.CommitSubmission{
		Kind:          storage.CommitKindAddPieces,
		TransactionID: "0xcommit",
		StatusURL:     "https://provider.example/commit/0xcommit",
		ProviderID:    sdktypes.NewBigInt(1),
		Identity: storage.ContextIdentity{
			Payer:        common.HexToAddress("0x1234"),
			ChainID:      sdktypes.ChainID(314159),
			RecordKeeper: common.HexToAddress("0x5678"),
		},
		PieceCIDs: []cid.Cid{cid.NewCidV1(cid.Raw, mh)},
	}
}
