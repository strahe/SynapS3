package synapse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/payments"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

type RemovalPreflight struct {
	DataSetMissing   bool
	EndEpoch         int64
	ReserveTopUp     *big.Int
	PayerUnderfunded bool
	Reason           string
}

type RemovalProvingConfig struct {
	MaxProvingPeriod int64
	ChallengeWindow  int64
}

// RemovalChainReader reads the service and payment conditions for a removal.
type RemovalChainReader interface {
	Preflight(context.Context, sdktypes.BigInt) (RemovalPreflight, error)
	ProvingConfig(context.Context) (RemovalProvingConfig, error)
}

type removalStorageReader interface {
	GetDataSet(context.Context, sdktypes.BigInt) (*warmstorage.DataSetInfo, error)
	GetPriceList(context.Context) (*warmstorage.PriceList, error)
	GetPDPConfig(context.Context) (*warmstorage.PDPConfig, error)
}

type removalPaymentsReader interface {
	AccountInfo(context.Context, common.Address, common.Address) (*payments.AccountState, error)
	ServiceApproval(context.Context, common.Address, common.Address, common.Address) (*payments.OperatorApproval, error)
	GetRail(context.Context, sdktypes.BigInt) (*payments.RailView, error)
}

type removalChainReader struct {
	dataSets removalStorageReader
	payments removalPaymentsReader
	identity storage.ContextIdentity
	epochs   ChainEpochReader
}

func NewRemovalChainReader(dataSets removalStorageReader, payerPayments removalPaymentsReader, identity storage.ContextIdentity, epochs ChainEpochReader) RemovalChainReader {
	return &removalChainReader{dataSets: dataSets, payments: payerPayments, identity: identity, epochs: epochs}
}

func (r *removalChainReader) Preflight(ctx context.Context, dataSetID sdktypes.BigInt) (RemovalPreflight, error) {
	result := RemovalPreflight{ReserveTopUp: new(big.Int)}
	if r.dataSets == nil || r.payments == nil || r.epochs == nil || r.identity.Payer == (common.Address{}) || r.identity.RecordKeeper == (common.Address{}) {
		return result, errors.New("removal chain reader is not configured")
	}
	info, err := r.dataSets.GetDataSet(ctx, dataSetID)
	if errors.Is(err, warmstorage.ErrNotFound) {
		result.DataSetMissing = true
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("reading removal data set: %w", err)
	}
	if info == nil {
		result.DataSetMissing = true
		return result, nil
	}
	if info.Payer != r.identity.Payer {
		return result, ErrServicePaidByAnother
	}
	if info.PDPEndEpoch > math.MaxInt64 {
		return result, errors.New("invalid removal data set end epoch")
	}
	result.EndEpoch = int64(info.PDPEndEpoch)
	if result.EndEpoch != 0 {
		return result, nil
	}
	prices, err := r.dataSets.GetPriceList(ctx)
	if err != nil {
		return result, fmt.Errorf("reading removal price list: %w", err)
	}
	if prices == nil || prices.Token == (common.Address{}) || !removalAmountsValid(info.PendingOneTimePayments, info.LifecycleReserveBalance,
		prices.Fees.SchedulePieceRemovalsFee, prices.Lockups.ReplenishThreshold, prices.Lockups.LifecycleReserveTarget, prices.Lockups.DefaultLockupPeriod) {
		return result, errors.New("removal reserve state is incomplete")
	}
	pending := new(big.Int).Add(info.PendingOneTimePayments, prices.Fees.SchedulePieceRemovalsFee)
	threshold := new(big.Int).Add(pending, prices.Lockups.ReplenishThreshold)
	if info.LifecycleReserveBalance.Cmp(threshold) >= 0 {
		return result, nil
	}
	target := new(big.Int).Add(prices.Lockups.LifecycleReserveTarget, pending)
	rail, err := r.payments.GetRail(ctx, info.PDPRailID)
	if err != nil {
		return result, fmt.Errorf("reading removal payment rail: %w", err)
	}
	if rail == nil || rail.Token != prices.Token || rail.From != r.identity.Payer || rail.Operator != r.identity.RecordKeeper ||
		!removalAmountsValid(rail.PaymentRate, rail.LockupPeriod, rail.LockupFixed, rail.EndEpoch) || rail.EndEpoch.Sign() != 0 {
		return result, errors.New("removal payment rail identity or lifecycle does not match the data set")
	}
	account, err := r.payments.AccountInfo(ctx, prices.Token, r.identity.Payer)
	if err != nil {
		return result, fmt.Errorf("reading removal payer account: %w", err)
	}
	approval, err := r.payments.ServiceApproval(ctx, prices.Token, r.identity.Payer, r.identity.RecordKeeper)
	if err != nil {
		return result, fmt.Errorf("reading removal payment approval: %w", err)
	}
	if account == nil || approval == nil || !removalAmountsValid(account.Funds, account.LockupCurrent, account.LockupRate, account.LockupLastSettledAt,
		approval.LockupAllowance, approval.LockupUsage, approval.MaxLockupPeriod) {
		return result, errors.New("removal payer account or approval is incomplete")
	}
	epoch, err := r.epochs.CurrentEpoch(ctx)
	if err != nil {
		return result, fmt.Errorf("reading removal payer epoch: %w", err)
	}
	if epoch < 0 {
		return result, errors.New("invalid removal payer epoch")
	}
	period := prices.Lockups.DefaultLockupPeriod
	oldLockup := new(big.Int).Mul(rail.PaymentRate, rail.LockupPeriod)
	oldLockup.Add(oldLockup, rail.LockupFixed)
	newLockup := new(big.Int).Mul(rail.PaymentRate, period)
	newLockup.Add(newLockup, target)
	delta := new(big.Int).Sub(newLockup, oldLockup)
	if delta.Sign() > 0 {
		result.ReserveTopUp.Set(delta)
	}
	elapsed := new(big.Int).Sub(big.NewInt(epoch), account.LockupLastSettledAt)
	if elapsed.Sign() < 0 {
		elapsed.SetInt64(0)
	}
	settledLockup := new(big.Int).Mul(account.LockupRate, elapsed)
	settledLockup.Add(settledLockup, account.LockupCurrent)
	// FilecoinPay settles the account before checking a fixed-lockup increase.
	// The stored settlement epoch alone does not establish an account deficit.
	fullySettled := settledLockup.Cmp(account.Funds) <= 0
	switch {
	case !fullySettled && (period.Cmp(rail.LockupPeriod) != 0 || target.Cmp(rail.LockupFixed) > 0):
		result.Reason = "payer account cannot settle its payment lockup through the current epoch"
	case period.Cmp(rail.LockupPeriod) > 0 && period.Cmp(approval.MaxLockupPeriod) > 0:
		result.Reason = "payment approval does not cover the required lockup period"
	case settledLockup.Cmp(oldLockup) < 0:
		return result, errors.New("payer payment lockup is smaller than the data set rail lockup")
	case new(big.Int).Add(settledLockup, delta).Cmp(account.Funds) > 0:
		result.Reason = "payer balance does not cover the required payment lockup"
	case delta.Sign() > 0 && new(big.Int).Add(approval.LockupUsage, delta).Cmp(approval.LockupAllowance) > 0:
		result.Reason = "payment approval does not cover the required lockup amount"
	}
	result.PayerUnderfunded = result.Reason != ""
	return result, nil
}

func (r *removalChainReader) ProvingConfig(ctx context.Context) (RemovalProvingConfig, error) {
	if r.dataSets == nil {
		return RemovalProvingConfig{}, errors.New("removal chain reader is not configured")
	}
	config, err := r.dataSets.GetPDPConfig(ctx)
	if err != nil {
		return RemovalProvingConfig{}, fmt.Errorf("reading removal proving configuration: %w", err)
	}
	if config == nil || config.MaxProvingPeriod == 0 || config.MaxProvingPeriod > math.MaxInt64 || config.ChallengeWindowSize == nil ||
		!config.ChallengeWindowSize.IsInt64() || config.ChallengeWindowSize.Sign() <= 0 || config.ChallengeWindowSize.Int64() >= int64(config.MaxProvingPeriod) {
		return RemovalProvingConfig{}, errors.New("invalid removal proving configuration")
	}
	return RemovalProvingConfig{MaxProvingPeriod: int64(config.MaxProvingPeriod), ChallengeWindow: config.ChallengeWindowSize.Int64()}, nil
}

func removalAmountsValid(values ...*big.Int) bool {
	for _, value := range values {
		if value == nil || value.Sign() < 0 {
			return false
		}
	}
	return true
}
