package admin

import (
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
)

// --- Response DTOs ---

type walletResponse struct {
	Configured     bool                `json:"configured"`
	Identity       *walletIdentityDTO  `json:"identity,omitempty"`
	Chain          *walletChainDTO     `json:"chain,omitempty"`
	WalletBalances *walletBalancesDTO  `json:"wallet_balances,omitempty"`
	PaymentAccount *paymentAccountDTO  `json:"payment_account,omitempty"`
	Contracts      *walletContractsDTO `json:"contracts,omitempty"`
	Business       *walletBusinessDTO  `json:"business,omitempty"`
	PartialErrors  map[string]string   `json:"partial_errors,omitempty"`
}

type walletIdentityDTO struct {
	Address string  `json:"address"`
	Nonce   *uint64 `json:"nonce"`
}

type walletChainDTO struct {
	Network              string  `json:"network"`
	ChainID              int64   `json:"chain_id"`
	CurrentEpoch         *string `json:"current_epoch"`
	EpochDurationSeconds int64   `json:"epoch_duration_seconds"`
}

type walletBalancesDTO struct {
	FILGas *string `json:"fil_gas"`
	USDFC  *string `json:"usdfc"`
}

type walletContractsDTO struct {
	PaymentsAddress string `json:"payments_address"`
	USDFCAddress    string `json:"usdfc_address"`
	USDFCDecimals   uint8  `json:"usdfc_decimals"`
}

type paymentAccountDTO struct {
	Funds               *string `json:"funds"`
	AvailableFunds      *string `json:"available_funds"`
	LockupCurrent       *string `json:"lockup_current"`
	LockupRate          *string `json:"lockup_rate"`
	LockupLastSettledAt *string `json:"lockup_last_settled_at"`
	FundedUntilEpoch    *string `json:"funded_until_epoch"`
	FundedUntilTime     *string `json:"funded_until_time,omitempty"`
	RunwaySeconds       *int64  `json:"runway_seconds,omitempty"`
	LockupRatePerDay    *string `json:"lockup_rate_per_day"`
	LockupRatePerMonth  *string `json:"lockup_rate_per_month"`
	NoActiveSpend       bool    `json:"no_active_spend"`
}

type walletBusinessDTO struct {
	DataSetCount          int `json:"data_set_count"`
	OnchainTasksPending   int `json:"onchain_tasks_pending"`
	OnchainTasksCompleted int `json:"onchain_tasks_completed"`
}

type walletOperationRequest struct {
	ClientRequestID string `json:"client_request_id"`
	Amount          string `json:"amount"`
}

type walletOperationResponse struct {
	Operation walletOperationDTO `json:"operation"`
}

type walletOperationsResponse struct {
	Operations []walletOperationDTO `json:"operations"`
}

type walletOperationDTO struct {
	ID              int64   `json:"id"`
	Type            string  `json:"type"`
	ClientRequestID string  `json:"client_request_id"`
	Amount          string  `json:"amount"`
	Status          string  `json:"status"`
	TxHash          *string `json:"tx_hash,omitempty"`
	LastError       *string `json:"last_error,omitempty"`
	LeaseUntil      *string `json:"lease_until,omitempty"`
	StartedAt       *string `json:"started_at,omitempty"`
	SubmittedAt     *string `json:"submitted_at,omitempty"`
	CompletedAt     *string `json:"completed_at,omitempty"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`
}

// --- Handler ---

func (s *Server) handleAPIWallet(w http.ResponseWriter, r *http.Request) {
	if s.wallet == nil {
		writeJSON(w, http.StatusOK, walletResponse{Configured: false})
		return
	}

	info, err := s.wallet.GetWalletInfo(r.Context())
	if err != nil {
		s.logger.Error("wallet query failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	resp := walletResponse{
		Configured: true,
		Identity: &walletIdentityDTO{
			Address: info.Address,
			Nonce:   info.Nonce,
		},
		Chain: &walletChainDTO{
			Network:              info.Network,
			ChainID:              info.ChainID,
			CurrentEpoch:         bigIntToString(info.CurrentEpoch),
			EpochDurationSeconds: info.EpochDurationSeconds,
		},
		WalletBalances: &walletBalancesDTO{
			FILGas: bigIntToString(info.FILGasBalance),
			USDFC:  bigIntToString(info.USDFCWalletBalance),
		},
		PaymentAccount: convertPaymentAccountDTO(info.PaymentAccount),
		Contracts: &walletContractsDTO{
			PaymentsAddress: info.PaymentsAddress,
			USDFCAddress:    info.USDFCAddress,
			USDFCDecimals:   info.USDFCDecimals,
		},
	}

	if len(info.Errors) > 0 {
		resp.PartialErrors = info.Errors
	}

	// Business stats (best-effort, from local DB).
	biz := &walletBusinessDTO{}
	if cnt, dbErr := s.repos.Buckets.CountStorageDataSets(r.Context()); dbErr != nil {
		s.logger.Warn("failed to count storage data sets", "error", dbErr)
		if resp.PartialErrors == nil {
			resp.PartialErrors = make(map[string]string)
		}
		resp.PartialErrors["data_set_count"] = "database query failed"
	} else {
		biz.DataSetCount = cnt
	}

	taskCounts, dbErr := s.repos.Tasks.CountByStatus(r.Context())
	if dbErr != nil {
		s.logger.Warn("failed to count tasks by status", "error", dbErr)
		if resp.PartialErrors == nil {
			resp.PartialErrors = make(map[string]string)
		}
		resp.PartialErrors["task_counts"] = "database query failed"
	}
	for _, tc := range taskCounts {
		if model.TaskType(tc.Type) != model.TaskTypeUpload {
			continue
		}
		switch tc.Status {
		case string(model.TaskStatusQueued), string(model.TaskStatusScheduled), string(model.TaskStatusWaiting), string(model.TaskStatusRunning):
			biz.OnchainTasksPending += int(tc.Count)
		case string(model.TaskStatusCompleted):
			biz.OnchainTasksCompleted += int(tc.Count)
		}
	}
	resp.Business = biz

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAPIWalletFund(w http.ResponseWriter, r *http.Request) {
	s.handleAPIWalletOperation(w, r, model.WalletOperationTypeFund)
}

func (s *Server) handleAPIWalletWithdraw(w http.ResponseWriter, r *http.Request) {
	s.handleAPIWalletOperation(w, r, model.WalletOperationTypeWithdraw)
}

func (s *Server) handleAPIWalletOperations(w http.ResponseWriter, r *http.Request) {
	if s.repos == nil || s.repos.WalletOperations == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "wallet operations unavailable"})
		return
	}
	limit := 20
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid limit"})
			return
		}
		limit = parsed
	}
	ops, err := s.repos.WalletOperations.ListRecent(r.Context(), limit)
	if err != nil {
		s.logger.Error("api: failed to list wallet operations", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	out := walletOperationsResponse{Operations: make([]walletOperationDTO, 0, len(ops))}
	for i := range ops {
		out.Operations = append(out.Operations, walletOperationToDTO(&ops[i]))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleAPIWalletOperation(w http.ResponseWriter, r *http.Request, opType model.WalletOperationType) {
	if s.wallet == nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "wallet is not configured"})
		return
	}
	if s.repos == nil || s.repos.WalletOperations == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "wallet operations unavailable"})
		return
	}
	if !s.settingsWritable() {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "wallet writes require loopback admin binding"})
		return
	}
	if r.Header.Get(settingsWriteHeader) != settingsWriteHeaderValue {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing settings write header"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "wallet writes require application/json"})
		return
	}

	var req walletOperationRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid wallet operation payload"})
		return
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid wallet operation payload"})
		return
	}

	clientRequestID := strings.TrimSpace(req.ClientRequestID)
	amount := strings.TrimSpace(req.Amount)
	if clientRequestID == "" || len(clientRequestID) > 128 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "client_request_id is required"})
		return
	}
	if !validBaseUnitAmount(amount) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "amount must be a positive integer string in USDFC base units"})
		return
	}

	op, _, err := s.repos.WalletOperations.CreateOrGet(r.Context(), repository.CreateWalletOperationInput{
		Type:            opType,
		ClientRequestID: clientRequestID,
		Amount:          amount,
	})
	if err != nil {
		if errors.Is(err, repository.ErrWalletOperationConflict) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "client_request_id already exists with a different amount"})
			return
		}
		s.logger.Error("api: failed to create wallet operation", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}

	dto := walletOperationToDTO(op)
	s.publishWalletOperation(dto)
	writeJSON(w, http.StatusAccepted, walletOperationResponse{Operation: dto})
}

// --- Helpers ---

func bigIntToString(v *big.Int) *string {
	if v == nil {
		return nil
	}
	s := v.String()
	return &s
}

func convertPaymentAccountDTO(acct *synapse.PaymentAccountInfo) *paymentAccountDTO {
	if acct == nil {
		return nil
	}
	return &paymentAccountDTO{
		Funds:               bigIntToString(acct.Funds),
		AvailableFunds:      bigIntToString(acct.AvailableFunds),
		LockupCurrent:       bigIntToString(acct.LockupCurrent),
		LockupRate:          bigIntToString(acct.LockupRate),
		LockupLastSettledAt: bigIntToString(acct.LockupLastSettledAt),
		FundedUntilEpoch:    bigIntToString(acct.FundedUntilEpoch),
		FundedUntilTime:     timeToString(acct.FundedUntilTime),
		RunwaySeconds:       acct.RunwaySeconds,
		LockupRatePerDay:    bigIntToString(acct.LockupRatePerDay),
		LockupRatePerMonth:  bigIntToString(acct.LockupRatePerMonth),
		NoActiveSpend:       acct.NoActiveSpend,
	}
}

func validBaseUnitAmount(amount string) bool {
	if amount == "" || amount[0] == '0' {
		return false
	}
	for _, r := range amount {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func walletOperationToDTO(op *model.WalletOperation) walletOperationDTO {
	if op == nil {
		return walletOperationDTO{}
	}
	return walletOperationDTO{
		ID:              op.ID,
		Type:            string(op.Type),
		ClientRequestID: op.ClientRequestID,
		Amount:          op.Amount,
		Status:          string(op.Status),
		TxHash:          op.TxHash,
		LastError:       op.LastError,
		LeaseUntil:      timeToString(op.LeaseUntil),
		StartedAt:       timeToString(op.StartedAt),
		SubmittedAt:     timeToString(op.SubmittedAt),
		CompletedAt:     timeToString(op.CompletedAt),
		CreatedAt:       op.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:       op.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func timeToString(v *time.Time) *string {
	if v == nil {
		return nil
	}
	out := v.UTC().Format(time.RFC3339Nano)
	return &out
}

func (s *Server) publishWalletOperation(op walletOperationDTO) {
	if s == nil || s.events == nil {
		return
	}
	s.events.Publish("wallet_operation_updated", map[string]any{
		"operation": op,
	})
}
