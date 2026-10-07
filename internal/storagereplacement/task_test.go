package storagereplacement

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/strahe/synaps3/internal/model"
)

func TestCoordinateInputRoundTrip(t *testing.T) {
	raw, err := json.Marshal(CoordinateInput{ReplacementID: 7, Generation: 2})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := ParseCoordinateInput(&model.Task{Input: raw})
	if err != nil {
		t.Fatalf("ParseCoordinateInput: %v", err)
	}
	if got != (CoordinateInput{ReplacementID: 7, Generation: 2}) {
		t.Fatalf("input = %#v", got)
	}
}

func TestReplacementInputsRejectIncompleteIdentity(t *testing.T) {
	if _, err := ParseCoordinateInput(nil); err == nil {
		t.Fatal("ParseCoordinateInput accepted nil task")
	}
	if _, err := ParseCoordinateInput(&model.Task{Input: json.RawMessage(`{"replacement_id":7}`)}); err == nil {
		t.Fatal("ParseCoordinateInput accepted missing generation")
	}
	if _, err := ParseRetireInput(&model.Task{Input: json.RawMessage(`{"replacement_id":7,"data_set_id":3}`)}); err == nil {
		t.Fatal("ParseRetireInput accepted missing generation")
	}
}

func TestReplacementTaskKeysAreGenerationScoped(t *testing.T) {
	if CoordinateTaskKey(1, 1) == CoordinateTaskKey(1, 2) {
		t.Fatal("replacement generations share a coordinator key")
	}
	if RetireTaskKey(4, 1) == RetireTaskKey(4, 2) {
		t.Fatal("retirement generations share a key")
	}
	if CoordinateTaskKey(1, 1) == RetireTaskKey(1, 1) {
		t.Fatal("coordinator and retirement keys collide")
	}
}

func TestCodeMapsReplacementErrors(t *testing.T) {
	cases := map[error]string{
		ErrActiveReplacement:  CodeActive,
		ErrSuperseded:         CodeSuperseded,
		ErrNotRetryable:       CodeNotRetryable,
		ErrTaskRunning:        CodeTaskRunning,
		ErrInvalidTarget:      CodeTargetInvalid,
		ErrTargetInUse:        CodeTargetInUse,
		ErrNoEligibleProvider: CodeNoEligibleProvider,
		ErrSourceNotCurrent:   CodeSourceNotCurrent,
	}
	seen := make(map[string]error, len(cases))
	for err, want := range cases {
		if got := Code(err); got != want {
			t.Fatalf("Code(%v) = %q, want %q", err, got, want)
		}
		if got := Code(errors.Join(errors.New("context"), err)); got != want {
			t.Fatalf("Code(wrapped %v) = %q, want %q", err, got, want)
		}
		if other, ok := seen[want]; ok {
			t.Fatalf("%v and %v share code %q", err, other, want)
		}
		seen[want] = err
	}
	if got := Code(errors.New("unrelated")); got != "" {
		t.Fatalf("Code(unrelated) = %q, want an empty string", got)
	}
}
