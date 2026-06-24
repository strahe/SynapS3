package e2e

import "testing"

func TestPlanReadinessAction(t *testing.T) {
	ready := readyReadiness()
	tests := []struct {
		name       string
		mutate     func(*ReadinessResult)
		state      ReadinessPlanState
		want       string
		wantAmount string
		wantErr    bool
	}{
		{
			name: "ready",
			want: ReadinessActionNone,
		},
		{
			name: "non critical warning allowed",
			mutate: func(r *ReadinessResult) {
				r.Checks = append(r.Checks, ReadinessCheck{ID: "payment_runway", Status: ReadinessWarning, Message: "short runway"})
			},
			want: ReadinessActionNone,
		},
		{
			name: "approval blocked",
			mutate: func(r *ReadinessResult) {
				setCheck(r, "fwss_approval", ReadinessBlocked, "")
			},
			want: ReadinessActionApprove,
		},
		{
			name: "funding blocked",
			mutate: func(r *ReadinessResult) {
				setCheck(r, "payment_funding", ReadinessBlocked, "100")
			},
			state:      ReadinessPlanState{USDFCWallet: "200"},
			want:       ReadinessActionFund,
			wantAmount: "100",
		},
		{
			name: "funding buffer",
			mutate: func(r *ReadinessResult) {
				setCheck(r, "payment_funding", ReadinessBlocked, "100")
			},
			state:      ReadinessPlanState{USDFCWallet: "200", DepositCap: "150", FundingBuffer: "75"},
			want:       ReadinessActionFund,
			wantAmount: "150",
		},
		{
			name: "both blocked prefers approval",
			mutate: func(r *ReadinessResult) {
				setCheck(r, "fwss_approval", ReadinessBlocked, "")
				setCheck(r, "payment_funding", ReadinessBlocked, "100")
			},
			state: ReadinessPlanState{USDFCWallet: "200"},
			want:  ReadinessActionApprove,
		},
		{
			name: "cap exceeded",
			mutate: func(r *ReadinessResult) {
				setCheck(r, "payment_funding", ReadinessBlocked, "300")
			},
			state:   ReadinessPlanState{USDFCWallet: "400", DepositCap: "200"},
			wantErr: true,
		},
		{
			name: "wallet balance insufficient",
			mutate: func(r *ReadinessResult) {
				setCheck(r, "payment_funding", ReadinessBlocked, "300")
			},
			state:   ReadinessPlanState{USDFCWallet: "200", DepositCap: "400"},
			wantErr: true,
		},
		{
			name: "critical unknown",
			mutate: func(r *ReadinessResult) {
				setCheck(r, "storage_cost", ReadinessUnknown, "")
			},
			wantErr: true,
		},
		{
			name: "critical missing",
			mutate: func(r *ReadinessResult) {
				r.Checks = r.Checks[:len(r.Checks)-1]
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ready
			result.Checks = append([]ReadinessCheck(nil), ready.Checks...)
			if tt.mutate != nil {
				tt.mutate(&result)
			}
			state := tt.state
			state.Readiness = result
			if state.USDFCWallet == "" {
				state.USDFCWallet = "1000"
			}
			plan, err := PlanReadinessAction(state)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("PlanReadinessAction succeeded: %#v", plan)
				}
				return
			}
			if err != nil {
				t.Fatalf("PlanReadinessAction: %v", err)
			}
			if plan.Action != tt.want {
				t.Fatalf("action = %s, want %s", plan.Action, tt.want)
			}
			if plan.Amount != tt.wantAmount {
				t.Fatalf("amount = %s, want %s", plan.Amount, tt.wantAmount)
			}
		})
	}
}

func readyReadiness() ReadinessResult {
	return ReadinessResult{Checks: []ReadinessCheck{
		{ID: "network_match", Status: ReadinessReady},
		{ID: "wallet_fil_gas", Status: ReadinessReady},
		{ID: "providers", Status: ReadinessReady},
		{ID: "storage_cost", Status: ReadinessReady},
		{ID: "payment_funding", Status: ReadinessReady},
		{ID: "fwss_approval", Status: ReadinessReady},
	}}
}

func setCheck(result *ReadinessResult, id, status, requiredUSDFC string) {
	for i := range result.Checks {
		if result.Checks[i].ID != id {
			continue
		}
		result.Checks[i].Status = status
		result.Checks[i].RequiredUSDFC = requiredUSDFC
		return
	}
	result.Checks = append(result.Checks, ReadinessCheck{ID: id, Status: status, RequiredUSDFC: requiredUSDFC})
}
