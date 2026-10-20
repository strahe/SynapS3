package synapse

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

const providerTerminationWaitTimeout = 30 * time.Second

type storageServiceTerminator interface {
	TerminateService(context.Context, sdktypes.BigInt, *storage.TerminateServiceOptions) (*storage.TerminateServiceResult, error)
}

// dataSetStateReader reads a data set's FWSS service state from the chain.
type dataSetStateReader interface {
	GetDataSet(context.Context, sdktypes.BigInt) (*warmstorage.DataSetInfo, error)
}

type terminationTransactionReader interface {
	TransactionReceipt(context.Context, common.Hash) (*ethtypes.Receipt, error)
	TransactionByHash(context.Context, common.Hash) (*ethtypes.Transaction, bool, error)
}

// ConfigureTerminationChain uses the existing chain connection to observe saved transactions.
func (s *StorageServiceAdapter) ConfigureTerminationChain(chain terminationTransactionReader) {
	s.terminationChain = chain
}

var (
	ErrTerminationEvidenceConflict  = errors.New("termination evidence conflicts with the saved transaction")
	ErrTerminationTransactionFailed = errors.New("termination transaction failed on chain")
)

// TerminationBlockedError means the service cannot be terminated until an
// operator resolves something the gateway must not decide on its own, such as
// settling outstanding payment debt. It is never retried automatically.
type TerminationBlockedError struct {
	Reason    string
	Shortfall *big.Int
	Err       error
}

func (e *TerminationBlockedError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Shortfall != nil {
		return fmt.Sprintf("service termination blocked (%s): outstanding amount %s", e.Reason, e.Shortfall)
	}
	return fmt.Sprintf("service termination blocked (%s)", e.Reason)
}

func (e *TerminationBlockedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IsTerminationBlocked reports whether termination needs operator action rather
// than another attempt.
func IsTerminationBlocked(err error) bool {
	var blocked *TerminationBlockedError
	return errors.As(err, &blocked)
}

// ErrServicePaidByAnother means the chain records the data set as paid for by a
// wallet other than the one signing. Under this configuration its number names
// someone else's service, so it is never terminated; an operator has to put the
// original wallet or network back.
var ErrServicePaidByAnother = errors.New("data set is paid for by another wallet")

// ErrTerminationObservationUnavailable identifies an unavailable provider status read.
// A failed lookup is neither evidence that an earlier transaction is absent nor permission
// to resend it. First-request fallback requires no historical submission evidence.
var ErrTerminationObservationUnavailable = errors.New("provider termination status unavailable")

// TerminateService is a destructive boundary used only after the retirement safety gate passes.
// It reads the payer-checked chain record before sending and records a known submission
// through onSubmitted before waiting. Direct fallback requires no submission evidence.
func (s *StorageServiceAdapter) TerminateService(ctx context.Context, dataSetID sdktypes.BigInt, onSubmitted func(string) error) (*TerminationResult, error) {
	return s.terminateService(ctx, dataSetID, onSubmitted, false)
}

// TerminateServiceDirect submits a fresh transaction after the caller has verified
// that the preceding attempt reverted and explicitly authorized another paid attempt.
func (s *StorageServiceAdapter) TerminateServiceDirect(ctx context.Context, dataSetID sdktypes.BigInt, onSubmitted func(string) error) (*TerminationResult, error) {
	return s.terminateService(ctx, dataSetID, onSubmitted, true)
}

func (s *StorageServiceAdapter) terminateService(ctx context.Context, dataSetID sdktypes.BigInt, onSubmitted func(string) error, skipProvider bool) (*TerminationResult, error) {
	if s == nil || s.terminator == nil || s.dataSets == nil {
		return nil, errors.New("data set terminator is not configured")
	}
	if ended, err := s.recordedTermination(ctx, dataSetID); err != nil || ended != nil {
		return ended, err
	}
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var evidence TerminationResult
	var callbackErr error
	callback := func(hash common.Hash) {
		if hash == (common.Hash{}) {
			return
		}
		if evidence.TxHash != "" && evidence.TxHash != hash.Hex() {
			callbackErr = ErrTerminationEvidenceConflict
			cancel()
			return
		}
		evidence.TxHash = hash.Hex()
		if onSubmitted != nil {
			callbackErr = onSubmitted(evidence.TxHash)
			if callbackErr != nil {
				cancel()
			}
		}
	}
	merge := func(res *storage.TerminateServiceResult, confirmed bool) {
		if res == nil {
			return
		}
		if (evidence.TxHash != "" && res.TxHash != nil && *res.TxHash != (common.Hash{}) && evidence.TxHash != res.TxHash.Hex()) ||
			(evidence.ConfirmedTxHash != "" && res.ConfirmedTxHash != nil && *res.ConfirmedTxHash != (common.Hash{}) && evidence.ConfirmedTxHash != res.ConfirmedTxHash.Hex()) {
			callbackErr = ErrTerminationEvidenceConflict
			return
		}
		if res.TxHash != nil && *res.TxHash != (common.Hash{}) {
			evidence.TxHash = res.TxHash.Hex()
		}
		if res.ConfirmedTxHash != nil && *res.ConfirmedTxHash != (common.Hash{}) {
			evidence.ConfirmedTxHash = res.ConfirmedTxHash.Hex()
		}
		if confirmed {
			epoch := int64(res.EndEpoch)
			evidence.EndEpoch = &epoch
			if evidence.ConfirmedTxHash != "" {
				status := uint64(ethtypes.ReceiptStatusSuccessful)
				evidence.ReceiptStatus = &status
			}
		}
	}
	res, err := s.terminator.TerminateService(requestCtx, dataSetID, &storage.TerminateServiceOptions{ProviderWaitTimeout: providerTerminationWaitTimeout, OnSubmitted: callback, SkipProvider: skipProvider})
	merge(res, err == nil)
	if callbackErr != nil {
		return &evidence, errors.Join(callbackErr, normalizeTerminationError(ctx, err))
	}
	if err != nil {
		providerErr := normalizeTerminationError(ctx, err)
		if skipProvider || evidence.TxHash != "" || evidence.ConfirmedTxHash != "" || IsTerminationBlocked(providerErr) || terminationPending(err) || ctx.Err() != nil {
			return &evidence, providerErr
		}
		ended, readErr := s.recordedTermination(ctx, dataSetID)
		if readErr != nil {
			return &evidence, fmt.Errorf("provider termination failed and service state is unknown: %w", errors.Join(providerErr, readErr))
		}
		if ended != nil {
			return ended, nil
		}
		res, err = s.terminator.TerminateService(requestCtx, dataSetID, &storage.TerminateServiceOptions{SkipProvider: true, OnSubmitted: callback})
		merge(res, err == nil)
		if callbackErr != nil {
			return &evidence, errors.Join(callbackErr, normalizeTerminationError(ctx, err))
		}
		if err != nil {
			directErr := NormalizeProviderOperationError(ctx, err)
			if evidence.TxHash == "" && evidence.ConfirmedTxHash == "" {
				if ended, readErr := s.recordedTermination(ctx, dataSetID); readErr == nil && ended != nil {
					return ended, nil
				}
			}
			return &evidence, fmt.Errorf("direct termination failed after provider relay error: %w", errors.Join(providerErr, directErr))
		}
	}
	if res == nil {
		return &evidence, errors.New("data set termination returned no result")
	}
	return &evidence, nil
}

// VerifyServicePayer checks ownership without sending a transaction.
func (s *StorageServiceAdapter) VerifyServicePayer(ctx context.Context, dataSetID sdktypes.BigInt) error {
	if s == nil || s.dataSets == nil {
		return errors.New("data set terminator is not configured")
	}
	_, err := s.recordedTermination(ctx, dataSetID)
	return err
}

// recordedTermination returns the recorded end of a service the chain already
// shows terminated, or nil while the service is still running.
func (s *StorageServiceAdapter) recordedTermination(ctx context.Context, dataSetID sdktypes.BigInt) (*TerminationResult, error) {
	info, err := s.dataSets.GetDataSet(ctx, dataSetID)
	if err != nil {
		return nil, fmt.Errorf("reading data set state: %w", err)
	}
	if info == nil {
		return nil, errors.New("reading data set state: no data set record")
	}
	// The direct path carries no payer check of its own, so a record that
	// belongs to someone else is refused here rather than terminated.
	if s.identity.Payer != (common.Address{}) && info.Payer != s.identity.Payer {
		return nil, fmt.Errorf("reading storage service state: data set %s is paid for by %s, not %s: %w",
			dataSetID.String(), info.Payer.Hex(), s.identity.Payer.Hex(), ErrServicePaidByAnother)
	}
	if info.PDPEndEpoch == 0 {
		return nil, nil
	}
	epoch := int64(info.PDPEndEpoch)
	return &TerminationResult{EndEpoch: &epoch}, nil
}

func terminationPending(err error) bool {
	var pending *pdp.TerminateServicePendingError
	return errors.As(err, &pending)
}

func normalizeTerminationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if debt, ok := errors.AsType[*storage.TerminateServiceDebtError](err); ok {
		return &TerminationBlockedError{Reason: "payment_debt", Shortfall: debt.Shortfall, Err: err}
	}
	// The provider accepted the request but has not published it yet. That
	// resolves on its own, so it is a dependency wait rather than a failure.
	if _, ok := errors.AsType[*pdp.TerminateServicePendingError](err); ok {
		return &ProviderUnavailableError{Cause: err}
	}
	return NormalizeProviderOperationError(ctx, err)
}

// ObserveTermination reads chain and provider evidence without sending a request.
func (s *StorageServiceAdapter) ObserveTermination(ctx context.Context, dataSetID sdktypes.BigInt, known TerminationResult) (*TerminationResult, bool, error) {
	return s.observeTermination(ctx, dataSetID, known, false)
}

// ObserveTerminationDirect checks the admitted transaction without consulting a
// provider request that can still describe a preceding failed attempt.
func (s *StorageServiceAdapter) ObserveTerminationDirect(ctx context.Context, dataSetID sdktypes.BigInt, known TerminationResult) (*TerminationResult, bool, error) {
	return s.observeTermination(ctx, dataSetID, known, true)
}

func (s *StorageServiceAdapter) observeTermination(ctx context.Context, dataSetID sdktypes.BigInt, known TerminationResult, skipProvider bool) (*TerminationResult, bool, error) {
	ended, err := s.recordedTermination(ctx, dataSetID)
	if err != nil || ended != nil {
		return ended, false, err
	}
	var observed *TerminationResult
	var pending bool
	var providerErr error
	if !skipProvider {
		observed, pending, providerErr = s.observeProviderTermination(ctx, dataSetID)
	}
	if known.TxHash == "" && known.ConfirmedTxHash == "" {
		if httpErr, ok := errors.AsType[*pdp.HTTPError](providerErr); ok && httpErr.StatusCode == http.StatusNotFound {
			return nil, false, nil
		}
		return observed, pending, providerErr
	}
	if observed != nil {
		if (known.TxHash != "" && observed.TxHash != "" && known.TxHash != observed.TxHash) ||
			(known.ConfirmedTxHash != "" && observed.ConfirmedTxHash != "" && known.ConfirmedTxHash != observed.ConfirmedTxHash) {
			return &known, true, errors.Join(ErrTerminationEvidenceConflict, providerErr)
		}
		if known.TxHash == "" {
			known.TxHash = observed.TxHash
		}
		if known.ConfirmedTxHash == "" {
			known.ConfirmedTxHash = observed.ConfirmedTxHash
		}
	}
	if s.terminationChain == nil {
		return &known, true, errors.Join(providerErr, errors.New("termination transaction reader is unavailable"))
	}
	hashText := known.ConfirmedTxHash
	if hashText == "" {
		hashText = known.TxHash
	}
	hashBytes, hashErr := hexutil.Decode(hashText)
	if hashErr != nil || len(hashBytes) != common.HashLength {
		return &known, true, fmt.Errorf("%w: invalid transaction hash", ErrTerminationEvidenceConflict)
	}
	hash := common.BytesToHash(hashBytes)
	readCtx, cancel := context.WithTimeout(ctx, defaultPDPStatusTimeout)
	defer cancel()
	receipt, receiptErr := s.terminationChain.TransactionReceipt(readCtx, hash)
	if receiptErr != nil && !errors.Is(receiptErr, ethereum.NotFound) {
		return &known, true, errors.Join(providerErr, fmt.Errorf("reading termination receipt %s: %w", hashText, receiptErr))
	}
	if receipt == nil {
		tx, txPending, txErr := s.terminationChain.TransactionByHash(readCtx, hash)
		if txErr != nil {
			return &known, true, errors.Join(providerErr, fmt.Errorf("reading termination transaction %s: %w", hashText, txErr))
		}
		if tx == nil || tx.Hash() != hash {
			return &known, true, errors.Join(providerErr, ErrTerminationEvidenceConflict)
		}
		if !txPending {
			return &known, true, errors.Join(providerErr, fmt.Errorf("termination transaction %s has no receipt", hashText))
		}
		return &known, true, providerErr
	}
	if receipt.TxHash != hash {
		return &known, true, errors.Join(providerErr, ErrTerminationEvidenceConflict)
	}
	known.ConfirmedTxHash = receipt.TxHash.Hex()
	if receipt.Status == ethtypes.ReceiptStatusFailed {
		known.ReceiptStatus = &receipt.Status
		return &known, true, errors.Join(providerErr, fmt.Errorf("%w: %s", ErrTerminationTransactionFailed, known.ConfirmedTxHash))
	}
	if receipt.Status != ethtypes.ReceiptStatusSuccessful {
		return &known, true, errors.Join(providerErr, fmt.Errorf("termination receipt has invalid status %d", receipt.Status))
	}
	known.ReceiptStatus = &receipt.Status
	ended, chainErr := s.recordedTermination(ctx, dataSetID)
	if ended != nil {
		known.EndEpoch = ended.EndEpoch
		return &known, false, nil
	}
	return &known, true, errors.Join(providerErr, chainErr)
}

func (s *StorageServiceAdapter) observeProviderTermination(ctx context.Context, dataSetID sdktypes.BigInt) (*TerminationResult, bool, error) {
	if s.service == nil {
		return nil, false, errors.New("data set status unavailable")
	}
	target, err := s.service.NewDataSetContext(ctx, dataSetID, storage.NewDataSetContextOptions{})
	if err != nil {
		return nil, false, err
	}
	client := s.providerHTTP
	if client == nil {
		client = NewProviderHTTPClient(defaultPDPStatusTimeout, false)
	}
	provider, err := pdp.New(target.ServiceURL(), pdp.WithHTTPClient(client), pdp.WithMaxRetries(0))
	if err != nil {
		return nil, false, err
	}
	pollCtx, cancel := context.WithTimeout(ctx, defaultPDPStatusTimeout)
	defer cancel()
	status, err := provider.GetTerminateServiceStatus(pollCtx, dataSetID)
	if httpErr, ok := errors.AsType[*pdp.HTTPError](err); ok && httpErr.StatusCode == 404 {
		return nil, false, err
	}
	if err != nil {
		normalized := NormalizeProviderOperationError(ctx, err)
		if ctx.Err() == nil && IsProviderUnavailable(normalized) {
			// Rate limiting is a request to wait, not evidence of an offline
			// provider. Keep authorization and malformed responses out too.
			if httpErr, ok := errors.AsType[*pdp.HTTPError](err); !ok ||
				httpErr.StatusCode == http.StatusRequestTimeout || httpErr.StatusCode >= http.StatusInternalServerError {
				return nil, false, fmt.Errorf("%w: %w", ErrTerminationObservationUnavailable, normalized)
			}
		}
		return nil, false, normalized
	}
	if status == nil {
		return nil, false, errors.New("termination observation returned no status")
	}
	evidence := &TerminationResult{}
	if status.TerminationTxHash != nil {
		evidence.TxHash = status.TerminationTxHash.Hex()
	}
	if status.ConfirmedTxHash != nil {
		evidence.ConfirmedTxHash = status.ConfirmedTxHash.Hex()
	}
	if evidence.TxHash == "" && evidence.ConfirmedTxHash == "" {
		return nil, true, nil
	}
	return evidence, true, nil
}
