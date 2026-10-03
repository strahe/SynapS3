package storagecommit

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultRequestTimeout = 15 * time.Second
	DefaultAttentionAfter = 15 * time.Minute
	// ChainRetryDelay is how long a request waits after a chain read or a
	// pre-send check could not complete.
	ChainRetryDelay = time.Minute
	// A provider keeps handling an add-pieces request for five minutes after
	// the client stops waiting, so a request whose outcome is unknown is sent
	// again only after that, doubling the wait per send up to 30 minutes.
	firstResendDelay = 5 * time.Minute
	maxResendDelay   = 30 * time.Minute
	// A refused request is tried again after one minute, doubling per
	// consecutive refusal up to 30 minutes.
	refusedRetryBaseDelay = time.Minute
	refusedRetryMaxDelay  = 30 * time.Minute
	proofReadConcurrency  = 8
)

// Commit is one request as the advancer acts on it.
type Commit struct {
	Request Request
	// Pieces are the request's piece CIDs in position order.
	Pieces []cid.Cid
	Target synapse.DataSetTarget
}

func (c Commit) extraData() ([]byte, error) {
	if c.Request.ExtraDataHex == nil || *c.Request.ExtraDataHex == "" {
		return nil, errors.New("storage commit request is not signed")
	}
	extraData, err := hex.DecodeString(*c.Request.ExtraDataHex)
	if err != nil {
		return nil, fmt.Errorf("decoding storage commit request: %w", err)
	}
	return extraData, nil
}

func (c Commit) pieceInputs() []storage.PieceInput {
	inputs := make([]storage.PieceInput, len(c.Pieces))
	for i, pieceCID := range c.Pieces {
		inputs[i] = storage.PieceInput{PieceCID: pieceCID}
	}
	return inputs
}

// Advancer decides what a request needs next from the provider's answers and
// the chain. It writes nothing: the caller records every outcome under its
// task fence.
type Advancer struct {
	// Nonces reads the chain record that proves where a request's pieces landed.
	Nonces         synapse.CommitNonceReader
	RequestTimeout time.Duration
	AttentionAfter time.Duration
	Now            func() time.Time
}

// ProofOutcome is what the chain says about a request's nonce.
type ProofOutcome uint8

const (
	// ProofUnused means the nonce was not consumed: no send of the request has
	// landed.
	ProofUnused ProofOutcome = iota
	// ProofLanded means the request's pieces are on chain, in order, at
	// consecutive IDs of its data set.
	ProofLanded
	// ProofConflict means the nonce was consumed somewhere other than the
	// request's pieces in its data set, or the request cannot be read.
	ProofConflict
)

type Proof struct {
	Outcome      ProofOutcome
	FirstPieceID sdktypes.BigInt
}

// Prove reads the request's FWSS nonce record. FWSS accepts a nonce once per
// payer and records `(next piece ID << 128) | data set ID`, so a consumed nonce
// names the piece IDs every send of the request could have produced. Every
// position is checked against PDPVerifier; a read that fails proves nothing.
func (a *Advancer) Prove(ctx context.Context, c Commit) (Proof, error) {
	if a.Nonces == nil {
		return Proof{}, errors.New("storage confirmation chain reader is unavailable")
	}
	extraData, err := c.extraData()
	if err != nil {
		return Proof{Outcome: ProofConflict}, nil
	}
	nonce, err := ExtraDataNonce(extraData)
	if err != nil {
		return Proof{Outcome: ProofConflict}, nil
	}
	nonceCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
	state, err := a.Nonces.ClientNonce(nonceCtx, nonce)
	cancel()
	if err != nil {
		return Proof{}, err
	}
	if !state.Consumed {
		return Proof{Outcome: ProofUnused}, nil
	}
	ref, bound := c.Target.DataSetRef()
	if !bound || !state.DataSetID.Equal(ref.DataSetID()) || len(c.Pieces) == 0 {
		return Proof{Outcome: ProofConflict}, nil
	}
	pieceIDs, err := state.PieceIDs(len(c.Pieces))
	if err != nil {
		return Proof{Outcome: ProofConflict}, nil
	}
	mismatch := make([]bool, len(pieceIDs))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(proofReadConcurrency)
	for i := range pieceIDs {
		group.Go(func() error {
			readCtx, cancel := context.WithTimeout(groupCtx, a.requestTimeout())
			defer cancel()
			pieceCID, err := a.Nonces.PieceCIDAt(readCtx, state.DataSetID, pieceIDs[i])
			if err != nil {
				return err
			}
			mismatch[i] = !pieceCID.Equals(c.Pieces[i])
			return nil
		})
	}
	readErr := group.Wait()
	for _, differs := range mismatch {
		if differs {
			return Proof{Outcome: ProofConflict}, nil
		}
	}
	if readErr != nil {
		return Proof{}, readErr
	}
	return Proof{Outcome: ProofLanded, FirstPieceID: pieceIDs[0]}, nil
}

// PrepareOutcome is what a ready request may do next.
type PrepareOutcome uint8

const (
	// PrepareSend means nothing of the request is on chain and its data set
	// accepts writes.
	PrepareSend PrepareOutcome = iota
	// PrepareConfirmed means an earlier send already landed.
	PrepareConfirmed
	// PrepareConflict means the request's nonce was consumed by other pieces.
	PrepareConflict
	// PrepareAbandon means the data set no longer accepts writes, so the request
	// can never land.
	PrepareAbandon
	// PrepareRetry means the chain could not be read; try again after
	// RetryAfter.
	PrepareRetry
)

type Prepared struct {
	Outcome    PrepareOutcome
	Proof      Proof
	Cause      error
	RetryAfter time.Duration
}

// Prepare checks a ready request before it is sent. The nonce is read first: a
// request that was sent before, or disclosed to the provider by a Pull, may
// already be on chain, and FWSS refuses its nonce again.
func (a *Advancer) Prepare(ctx context.Context, c Commit) Prepared {
	proof, err := a.Prove(ctx, c)
	if err != nil {
		return Prepared{Outcome: PrepareRetry, Cause: err, RetryAfter: ChainRetryDelay}
	}
	switch proof.Outcome {
	case ProofLanded:
		return Prepared{Outcome: PrepareConfirmed, Proof: proof}
	case ProofConflict:
		return Prepared{Outcome: PrepareConflict}
	}
	// The SDK checks the data set again while submitting; checking first keeps
	// a data set that refuses writes, or a chain that cannot be read, from
	// leaving a send whose outcome has to be recovered.
	checkCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
	err = c.Target.CheckWritable(checkCtx)
	cancel()
	if err != nil {
		if DataSetRefusesWrites(err) {
			return Prepared{Outcome: PrepareAbandon, Cause: err}
		}
		return Prepared{Outcome: PrepareRetry, Cause: err, RetryAfter: ChainRetryDelay}
	}
	return Prepared{Outcome: PrepareSend}
}

// SendKind classifies what one SubmitCommit call left behind.
type SendKind uint8

const (
	// SendAccepted means the provider returned a submission.
	SendAccepted SendKind = iota
	// SendRefused means the provider answered with a 4xx. Curio answers every
	// add-pieces 4xx before sending that call's transaction.
	SendRefused
	// SendNotSent means the SDK refused the data set before contacting the
	// provider.
	SendNotSent
	// SendUnknown means a transaction may exist.
	SendUnknown
)

type SendResult struct {
	Kind       SendKind
	Submission *storage.CommitSubmission
	Err        error
}

// Send submits the request's signed authorization once. onSubmitted runs when
// the SDK announces the provider's submission, whatever the call returns.
func (a *Advancer) Send(ctx context.Context, c Commit, onSubmitted func(storage.CommitSubmission)) SendResult {
	extraData, err := c.extraData()
	if err != nil {
		return SendResult{Kind: SendNotSent, Err: err}
	}
	observed := false
	submission, err := c.Target.SubmitCommit(ctx, storage.CommitRequest{
		Pieces:    c.pieceInputs(),
		ExtraData: extraData,
		OnSubmitted: func(submission storage.CommitSubmission) {
			observed = true
			if onSubmitted != nil {
				onSubmitted(submission)
			}
		},
	})
	if err == nil {
		if submission == nil {
			return SendResult{Kind: SendUnknown}
		}
		return SendResult{Kind: SendAccepted, Submission: submission}
	}
	if observed || context.Cause(ctx) != nil {
		return SendResult{Kind: SendUnknown, Submission: submission, Err: err}
	}
	if DataSetRefusesWrites(err) {
		return SendResult{Kind: SendNotSent, Err: err}
	}
	if synapse.ClassifyCommitRejection(err) != synapse.CommitNotRejected {
		return SendResult{Kind: SendRefused, Err: err}
	}
	return SendResult{Kind: SendUnknown, Err: err}
}

// ObserveKind is what a submitted request needs next.
type ObserveKind uint8

const (
	// ObservePending means check again after RetryAfter, or the caller's poll
	// interval when it is zero.
	ObservePending ObserveKind = iota
	// ObserveConfirmed means the chain proves the request.
	ObserveConfirmed
	// ObserveResendDue means nothing of the request is on chain and the latest
	// send is old enough to send the same request again.
	ObserveResendDue
	// ObserveStop means the request's nonce was consumed by other pieces.
	// Nothing more is sent until an operator recovers it.
	ObserveStop
	// ObserveAbandon means the data set no longer accepts writes and nothing of
	// the request is on chain.
	ObserveAbandon
)

type Observation struct {
	Kind         ObserveKind
	RetryAfter   time.Duration
	Confirmation *Confirmation
	// Attention, when set, is flagged on the request with this observation.
	Attention AttentionCode
	// DropEvidence clears the recorded transaction and status URL: the
	// provider's receipt for them did not describe this request.
	DropEvidence bool
	Cause        error
}

// Confirmation is the chain proof of a request with the submission evidence
// that accompanied it.
type Confirmation struct {
	// TransactionID is the request's latest recorded submission, if any.
	TransactionID string
	// ConfirmedTransactionID is set only when the provider reported the
	// transaction that included the pieces. A request proven by its nonce alone
	// may have landed in a different send.
	ConfirmedTransactionID string
	FirstPieceID           sdktypes.BigInt
}

// Observe resolves a submitted request. The provider's receipt is only a hint:
// a request is confirmed by its nonce record, and when the provider cannot
// answer, disagrees, or rejects it, the chain decides.
func (a *Advancer) Observe(ctx context.Context, c Commit) (Observation, error) {
	if context.Cause(ctx) != nil {
		return Observation{Kind: ObservePending}, nil
	}
	req := c.Request
	if req.StatusURL == nil || *req.StatusURL == "" {
		return a.unobserved(ctx, c, false)
	}
	statusCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
	status, err := c.Target.GetCommitStatus(statusCtx, *req.StatusURL)
	cancel()
	if err != nil {
		if context.Cause(ctx) != nil {
			return Observation{Kind: ObservePending}, nil
		}
		if errors.Is(err, storage.ErrInvalidArgument) || errors.Is(err, pdp.ErrInvalidStatus) {
			return a.unobserved(ctx, c, true)
		}
		code := AttentionConfirmationTimeout
		if errors.Is(err, storage.ErrDataSetUnavailable) || synapse.IsDataSetServiceEnded(err) {
			code = AttentionDataSetUnavailable
		}
		if req.AttentionAt != nil {
			// A provider that still cannot answer once the request needs
			// attention leaves no evidence at all: the chain decides, and an
			// unused nonce is sent again as if no reply had come back.
			return a.unobserved(ctx, c, false)
		}
		return a.byProof(ctx, c, func() (Observation, error) {
			return a.pending(req, code), nil
		})
	}
	if status == nil {
		return a.pending(req, AttentionConfirmationTimeout), nil
	}
	ref, bound := c.Target.DataSetRef()
	if !bound || status.DataSet == nil || !status.DataSet.Equal(ref) || status.Kind != storage.CommitKindAddPieces ||
		req.TransactionID == nil || status.TransactionID != *req.TransactionID {
		return a.unobserved(ctx, c, true)
	}
	switch status.State {
	case storage.CommitStatePending:
		// A provider still tracking its transaction is trusted until the request
		// is old enough to need attention; past that, the transaction it reports
		// may have been replaced by one the chain did include.
		if req.AttentionAt == nil && !a.attentionDue(req) {
			return Observation{Kind: ObservePending}, nil
		}
		return a.byProof(ctx, c, func() (Observation, error) {
			return a.pending(req, AttentionConfirmationTimeout), nil
		})
	case storage.CommitStateConfirmed:
		if len(status.PieceIDs) != len(c.Pieces) || !contiguous(status.PieceIDs) {
			return a.unobserved(ctx, c, true)
		}
		proof, err := a.Prove(ctx, c)
		if err != nil {
			return Observation{Kind: ObservePending, RetryAfter: ChainRetryDelay, Cause: err}, nil
		}
		switch proof.Outcome {
		case ProofLanded:
			confirmation := Confirmation{
				TransactionID:          status.TransactionID,
				ConfirmedTransactionID: status.TransactionID,
				FirstPieceID:           proof.FirstPieceID,
			}
			if status.ConfirmedTransactionID != "" {
				confirmation.ConfirmedTransactionID = status.ConfirmedTransactionID
			}
			return Observation{Kind: ObserveConfirmed, Confirmation: &confirmation}, nil
		case ProofConflict:
			return Observation{Kind: ObserveStop, Attention: AttentionSubmissionMismatch}, nil
		default:
			// The provider saw the transaction before the chain view this node
			// reads; the nonce settles it once visible.
			return a.pending(req, AttentionConfirmationTimeout), nil
		}
	default:
		// A rejected transaction, or a state this version does not know, says
		// nothing about whether another send of the same request landed.
		return a.unobserved(ctx, c, true)
	}
}

// byProof settles a request on its nonce record. When the chain shows nothing,
// or cannot be read, fallback decides.
func (a *Advancer) byProof(ctx context.Context, c Commit, fallback func() (Observation, error)) (Observation, error) {
	proof, err := a.Prove(ctx, c)
	if err != nil {
		return fallback()
	}
	switch proof.Outcome {
	case ProofLanded:
		return a.confirmedByNonce(c, proof), nil
	case ProofConflict:
		return Observation{Kind: ObserveStop, Attention: AttentionSubmissionMismatch}, nil
	default:
		return fallback()
	}
}

// unobserved resolves a request without provider evidence. The chain decides:
// a consumed nonce proves where the pieces landed, and an unused one lets the
// same request be sent again, which FWSS can accept at most once.
func (a *Advancer) unobserved(ctx context.Context, c Commit, dropEvidence bool) (Observation, error) {
	proof, err := a.Prove(ctx, c)
	if err != nil {
		return Observation{Kind: ObservePending, RetryAfter: ChainRetryDelay, DropEvidence: dropEvidence, Cause: err}, nil
	}
	switch proof.Outcome {
	case ProofLanded:
		return a.confirmedByNonce(c, proof), nil
	case ProofConflict:
		return Observation{Kind: ObserveStop, Attention: AttentionSubmissionMismatch}, nil
	}
	checkCtx, cancel := context.WithTimeout(ctx, a.requestTimeout())
	writable := c.Target.CheckWritable(checkCtx)
	cancel()
	if DataSetRefusesWrites(writable) {
		return Observation{Kind: ObserveAbandon, Cause: writable}, nil
	}
	req := c.Request
	observation := Observation{Kind: ObserveResendDue, DropEvidence: dropEvidence}
	// Like a provider still reporting pending, a request unregistered past the
	// attention threshold is flagged while recovery keeps sending it.
	if req.AttentionAt == nil && a.attentionDue(req) {
		observation.Attention = AttentionConfirmationTimeout
	}
	if req.LastSentAt != nil {
		if wait := req.LastSentAt.Add(ResendDelay(req.Sends)).Sub(a.now()); wait > 0 {
			observation.Kind = ObservePending
			observation.RetryAfter = wait
		}
	}
	return observation, nil
}

// confirmedByNonce settles a request on the chain's nonce record. A recorded
// transaction stays the request's submission, but the transaction that actually
// included the pieces may be another one, so none is confirmed.
func (a *Advancer) confirmedByNonce(c Commit, proof Proof) Observation {
	confirmation := Confirmation{FirstPieceID: proof.FirstPieceID}
	if c.Request.TransactionID != nil {
		confirmation.TransactionID = *c.Request.TransactionID
	}
	return Observation{Kind: ObserveConfirmed, Confirmation: &confirmation}
}

func (a *Advancer) pending(req Request, code AttentionCode) Observation {
	observation := Observation{Kind: ObservePending}
	if req.AttentionAt == nil && a.attentionDue(req) {
		observation.Attention = code
	}
	return observation
}

func (a *Advancer) attentionDue(req Request) bool {
	return req.SubmittedAt != nil && a.now().Sub(*req.SubmittedAt) >= a.attentionAfter()
}

func contiguous(ids []sdktypes.BigInt) bool {
	if len(ids) == 0 {
		return false
	}
	first := ids[0].Big()
	for i, id := range ids {
		if id.Big().Cmp(new(big.Int).Add(first, big.NewInt(int64(i)))) != 0 {
			return false
		}
	}
	return true
}

// DataSetRefusesWrites reports whether err means the data set will not accept
// a commit, because its storage service ended or its PDP payment did. Both are
// read from the chain before the provider is contacted.
func DataSetRefusesWrites(err error) bool {
	return errors.Is(err, storage.ErrDataSetUnavailable) ||
		synapse.IsDataSetServiceEnded(err) ||
		synapse.IsDataSetWriteBlocked(err)
}

// ResendDelay is how long a request whose outcome is unknown waits after its
// latest send before it is sent again.
func ResendDelay(sends int) time.Duration {
	delay := firstResendDelay
	for i := 1; i < sends && delay < maxResendDelay; i++ {
		delay *= 2
	}
	return min(delay, maxResendDelay)
}

// RefusedRetryDelay is how long a request waits after its refusals-th
// consecutive refusal.
func RefusedRetryDelay(refusals int) time.Duration {
	delay := refusedRetryBaseDelay
	for i := 1; i < refusals && delay < refusedRetryMaxDelay; i++ {
		delay *= 2
	}
	return min(delay, refusedRetryMaxDelay)
}

func (a *Advancer) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

func (a *Advancer) requestTimeout() time.Duration {
	if a.RequestTimeout > 0 {
		return a.RequestTimeout
	}
	return DefaultRequestTimeout
}

func (a *Advancer) attentionAfter() time.Duration {
	if a.AttentionAfter > 0 {
		return a.AttentionAfter
	}
	return DefaultAttentionAfter
}
