package admin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

type WarmStorageMarket interface {
	GetPriceList(context.Context) (*warmstorage.PriceList, error)
	IsProviderApproved(context.Context, sdktypes.BigInt) (bool, error)
	FWSSAddress() common.Address
}

type warmStoragePriceView struct {
	ChainID        uint64            `json:"chain_id"`
	FWSSAddress    string            `json:"fwss_address"`
	Token          string            `json:"token"`
	SupportedToken bool              `json:"supported_token"`
	Rates          map[string]string `json:"rates"`
	Fees           map[string]string `json:"fees"`
	Lockups        map[string]string `json:"lockups"`
	ObservedAt     time.Time         `json:"observed_at"`
	Fingerprint    string            `json:"fingerprint"`
}

func (s *Server) WithWarmStorageMarket(market WarmStorageMarket, chainID uint64, usdfcAddress string) *Server {
	s.warmStorageMarket = market
	s.marketChainID = chainID
	s.usdfcAddress = usdfcAddress
	return s
}

func (s *Server) currentWarmStoragePrice(ctx context.Context) (warmStoragePriceView, error) {
	if s.warmStorageMarket == nil {
		return warmStoragePriceView{}, errors.New("warm storage market unavailable")
	}
	readCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	list, err := s.warmStorageMarket.GetPriceList(readCtx)
	if err != nil || list == nil {
		return warmStoragePriceView{}, errors.New("warm storage price unavailable")
	}
	view := warmStoragePriceView{
		ChainID: s.marketChainID, FWSSAddress: s.warmStorageMarket.FWSSAddress().Hex(), Token: list.Token.Hex(),
		SupportedToken: strings.EqualFold(list.Token.Hex(), s.usdfcAddress),
		Rates: map[string]string{
			"storage_per_tib_per_month": marketAmount(list.Rates.StoragePerTiBPerMonth),
			"dataset_fee_per_month":     marketAmount(list.Rates.DatasetFeePerMonth),
			"cdn_egress_per_tib":        marketAmount(list.Rates.CDNEgressPerTiB),
			"cache_miss_egress_per_tib": marketAmount(list.Rates.CacheMissEgressPerTiB),
		},
		Fees: map[string]string{
			"create_data_set":         marketAmount(list.Fees.CreateDataSetFee),
			"add_pieces_base":         marketAmount(list.Fees.AddPiecesBaseFee),
			"add_pieces_per_piece":    marketAmount(list.Fees.AddPiecesPerPieceFee),
			"schedule_piece_removals": marketAmount(list.Fees.SchedulePieceRemovalsFee),
			"terminate":               marketAmount(list.Fees.TerminateFee),
		},
		Lockups: map[string]string{
			"lifecycle_reserve_target": marketAmount(list.Lockups.LifecycleReserveTarget),
			"replenish_threshold":      marketAmount(list.Lockups.ReplenishThreshold),
			"default_lockup_period":    marketAmount(list.Lockups.DefaultLockupPeriod),
			"cdn_lockup_amount":        marketAmount(list.Lockups.CDNLockupAmount),
			"cache_miss_lockup_amount": marketAmount(list.Lockups.CacheMissLockupAmount),
			"cdn_lockup_period":        marketAmount(list.Lockups.CDNLockupPeriod),
		},
	}
	for _, group := range []map[string]string{view.Rates, view.Fees, view.Lockups} {
		for _, value := range group {
			if value == "" {
				return warmStoragePriceView{}, errors.New("incomplete warm storage price")
			}
		}
	}
	canonical := view
	canonical.SupportedToken = false
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return warmStoragePriceView{}, err
	}
	digest := sha256.Sum256(encoded)
	view.Fingerprint = hex.EncodeToString(digest[:])
	view.ObservedAt = time.Now().UTC()
	return view, nil
}

func marketAmount(value *big.Int) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func (s *Server) handleAPIWarmStoragePrice(w http.ResponseWriter, r *http.Request) {
	price, err := s.currentWarmStoragePrice(r.Context())
	if err != nil {
		s.logger.Warn("api: warm storage price unavailable", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "price list unavailable"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, price)
}
