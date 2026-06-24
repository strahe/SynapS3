package e2e

import (
	"fmt"
	"math/big"
	"slices"
)

const (
	ReadinessReady   = "ready"
	ReadinessWarning = "warning"
	ReadinessBlocked = "blocked"
	ReadinessUnknown = "unknown"

	ReadinessActionNone    = "none"
	ReadinessActionApprove = "approve"
	ReadinessActionFund    = "fund"

	DepositCapUSDFCBaseUnits = "2000000000000000000"
	FundingBufferBaseUnits   = "500000000000000000"
)

var CriticalReadinessChecks = []string{
	"network_match",
	"wallet_fil_gas",
	"providers",
	"storage_cost",
	"payment_funding",
	"fwss_approval",
}

type ReadinessPlanState struct {
	Readiness        ReadinessResult
	USDFCWallet      string
	ApproveAttempted bool
	FundAttempted    bool
	DepositCap       string
	FundingBuffer    string
}

type ReadinessPlan struct {
	Action string
	Amount string
	Ready  bool
}

func PlanReadinessAction(state ReadinessPlanState) (ReadinessPlan, error) {
	capText := state.DepositCap
	if capText == "" {
		capText = DepositCapUSDFCBaseUnits
	}
	checks := make(map[string]ReadinessCheck, len(state.Readiness.Checks))
	for _, check := range state.Readiness.Checks {
		checks[check.ID] = check
	}
	for _, id := range CriticalReadinessChecks {
		check, ok := checks[id]
		if !ok {
			return ReadinessPlan{}, fmt.Errorf("readiness check %q is missing", id)
		}
		if check.Status == ReadinessUnknown {
			return ReadinessPlan{}, fmt.Errorf("readiness check %q is unknown: %s", id, check.Message)
		}
	}
	if blocked(checks, "network_match") {
		return ReadinessPlan{}, fmt.Errorf("calibration network mismatch: %s", checks["network_match"].Message)
	}
	if blocked(checks, "wallet_fil_gas") {
		return ReadinessPlan{}, fmt.Errorf("FIL gas is not available: %s", checks["wallet_fil_gas"].Message)
	}
	if blocked(checks, "providers") {
		return ReadinessPlan{}, fmt.Errorf("not enough approved active providers: %s", checks["providers"].Message)
	}
	if blocked(checks, "storage_cost") {
		return ReadinessPlan{}, fmt.Errorf("storage cost estimate is blocked: %s", checks["storage_cost"].Message)
	}

	approval := checks["fwss_approval"]
	funding := checks["payment_funding"]
	if approval.Status == ReadinessBlocked {
		if state.ApproveAttempted {
			return ReadinessPlan{}, fmt.Errorf("FWSS approval is still blocked after approve operation: %s", approval.Message)
		}
		return ReadinessPlan{Action: ReadinessActionApprove}, nil
	}
	if funding.Status == ReadinessBlocked {
		if state.FundAttempted {
			return ReadinessPlan{}, fmt.Errorf("payment funding is still blocked after fund operation: %s", funding.Message)
		}
		if funding.RequiredUSDFC == "" {
			return ReadinessPlan{}, fmt.Errorf("payment funding is blocked without required_usdfc")
		}
		amountText, err := BufferedFundingAmount(funding.RequiredUSDFC, capText, state.FundingBuffer)
		if err != nil {
			return ReadinessPlan{}, err
		}
		wallet, ok := new(big.Int).SetString(state.USDFCWallet, 10)
		if !ok {
			return ReadinessPlan{}, fmt.Errorf("USDFC wallet balance is unavailable")
		}
		amount, ok := new(big.Int).SetString(amountText, 10)
		if !ok {
			return ReadinessPlan{}, fmt.Errorf("invalid planned funding %q", amountText)
		}
		if wallet.Cmp(amount) < 0 {
			return ReadinessPlan{}, fmt.Errorf("USDFC wallet balance %s is less than planned funding %s", wallet.String(), amount.String())
		}
		return ReadinessPlan{Action: ReadinessActionFund, Amount: amount.String()}, nil
	}

	for _, id := range CriticalReadinessChecks {
		if checks[id].Status != ReadinessReady {
			return ReadinessPlan{}, fmt.Errorf("readiness check %q is %s, want ready", id, checks[id].Status)
		}
	}
	return ReadinessPlan{Action: ReadinessActionNone, Ready: true}, nil
}

// BufferedFundingAmount adds an optional buffer without reducing the required
// amount to fit the transaction cap.
func BufferedFundingAmount(requiredText, capText, bufferText string) (string, error) {
	required, ok := new(big.Int).SetString(requiredText, 10)
	if !ok || required.Sign() <= 0 {
		return "", fmt.Errorf("invalid required funding %q", requiredText)
	}
	capAmount, ok := new(big.Int).SetString(capText, 10)
	if !ok || capAmount.Sign() <= 0 {
		return "", fmt.Errorf("invalid deposit cap %q", capText)
	}
	if required.Cmp(capAmount) > 0 {
		return "", fmt.Errorf("required funding %s exceeds cap %s", required.String(), capAmount.String())
	}
	amount := new(big.Int).Set(required)
	if bufferText == "" {
		return amount.String(), nil
	}
	buffer, ok := new(big.Int).SetString(bufferText, 10)
	if !ok || buffer.Sign() < 0 {
		return "", fmt.Errorf("invalid funding buffer %q", bufferText)
	}
	amount.Add(amount, buffer)
	if amount.Cmp(capAmount) > 0 {
		amount.Set(capAmount)
	}
	return amount.String(), nil
}

func blocked(checks map[string]ReadinessCheck, id string) bool {
	return checks[id].Status == ReadinessBlocked
}

func CriticalReadinessSatisfied(result ReadinessResult) bool {
	for _, id := range CriticalReadinessChecks {
		check, ok := result.Check(id)
		if !ok || check.Status != ReadinessReady {
			return false
		}
	}
	return true
}

func NonCriticalWarnings(result ReadinessResult) []ReadinessCheck {
	var warnings []ReadinessCheck
	for _, check := range result.Checks {
		if check.Status == ReadinessWarning && !slices.Contains(CriticalReadinessChecks, check.ID) {
			warnings = append(warnings, check)
		}
	}
	return warnings
}
