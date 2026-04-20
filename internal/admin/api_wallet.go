package admin

import (
	"math/big"
	"net/http"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
)

// --- Response DTOs ---

type walletResponse struct {
	Configured      bool               `json:"configured"`
	Address         string             `json:"address,omitempty"`
	Network         string             `json:"network,omitempty"`
	ChainID         int64              `json:"chain_id,omitempty"`
	Nonce           *uint64            `json:"nonce"`
	PaymentsAddress string             `json:"payments_address,omitempty"`
	USDFCAddress    string             `json:"usdfc_address,omitempty"`
	USDFCDecimals   uint8              `json:"usdfc_decimals"`
	FILBalance      *string            `json:"fil_balance"`
	USDFCBalance    *string            `json:"usdfc_balance"`
	FILAccount      *tokenAccountDTO   `json:"fil_account"`
	USDFCAccount    *tokenAccountDTO   `json:"usdfc_account"`
	Business        *walletBusinessDTO `json:"business,omitempty"`
	PartialErrors   map[string]string  `json:"partial_errors,omitempty"`
}

type tokenAccountDTO struct {
	Funds               *string `json:"funds"`
	AvailableFunds      *string `json:"available_funds"`
	LockupCurrent       *string `json:"lockup_current"`
	LockupRate          *string `json:"lockup_rate"`
	LockupLastSettledAt *string `json:"lockup_last_settled_at"`
	FundedUntilEpoch    *string `json:"funded_until_epoch"`
}

type walletBusinessDTO struct {
	ProofSetCount         int `json:"proof_set_count"`
	OnchainTasksPending   int `json:"onchain_tasks_pending"`
	OnchainTasksCompleted int `json:"onchain_tasks_completed"`
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
		Configured:      true,
		Address:         info.Address,
		Network:         info.Network,
		ChainID:         info.ChainID,
		Nonce:           info.Nonce,
		PaymentsAddress: info.PaymentsAddress,
		USDFCAddress:    info.USDFCAddress,
		USDFCDecimals:   info.USDFCDecimals,
		FILBalance:      bigIntToString(info.FILBalance),
		USDFCBalance:    bigIntToString(info.USDFCBalance),
		FILAccount:      convertTokenAccountDTO(info.FILAccount),
		USDFCAccount:    convertTokenAccountDTO(info.USDFCAccount),
	}

	if len(info.Errors) > 0 {
		resp.PartialErrors = info.Errors
	}

	// Business stats (best-effort, from local DB).
	biz := &walletBusinessDTO{}
	if cnt, dbErr := s.repos.Buckets.CountWithProofSet(r.Context()); dbErr != nil {
		s.logger.Warn("failed to count buckets with proof set", "error", dbErr)
		if resp.PartialErrors == nil {
			resp.PartialErrors = make(map[string]string)
		}
		resp.PartialErrors["proof_set_count"] = "database query failed"
	} else {
		biz.ProofSetCount = cnt
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
		case string(model.TaskStatusPending), string(model.TaskStatusRunning):
			biz.OnchainTasksPending += int(tc.Count)
		case string(model.TaskStatusCompleted):
			biz.OnchainTasksCompleted += int(tc.Count)
		}
	}
	resp.Business = biz

	writeJSON(w, http.StatusOK, resp)
}

// --- Helpers ---

func bigIntToString(v *big.Int) *string {
	if v == nil {
		return nil
	}
	s := v.String()
	return &s
}

func convertTokenAccountDTO(acct *synapse.TokenAccountInfo) *tokenAccountDTO {
	if acct == nil {
		return nil
	}
	return &tokenAccountDTO{
		Funds:               bigIntToString(acct.Funds),
		AvailableFunds:      bigIntToString(acct.AvailableFunds),
		LockupCurrent:       bigIntToString(acct.LockupCurrent),
		LockupRate:          bigIntToString(acct.LockupRate),
		LockupLastSettledAt: bigIntToString(acct.LockupLastSettledAt),
		FundedUntilEpoch:    bigIntToString(acct.FundedUntilEpoch),
	}
}
