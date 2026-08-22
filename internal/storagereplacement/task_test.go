package storagereplacement

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/model"
)

func TestMigratePayloadRoundTrip(t *testing.T) {
	task := &model.Task{Payload: NewMigratePayload(7, 42, 99)}
	got, err := ParseMigratePayload(task)
	if err != nil {
		t.Fatalf("ParseMigratePayload: %v", err)
	}
	want := MigratePayload{ReplacementID: 7, ItemID: 42, CopyID: 99}
	if got != want {
		t.Fatalf("payload = %+v, want %+v", got, want)
	}
}

// Between items the coordinator holds no item and no copy, and must still be
// decodable.
func TestMigratePayloadWithoutAssignedItem(t *testing.T) {
	task := &model.Task{Payload: NewMigratePayload(7, 0, 0)}
	got, err := ParseMigratePayload(task)
	if err != nil {
		t.Fatalf("ParseMigratePayload: %v", err)
	}
	if got.ItemID != 0 || got.CopyID != 0 {
		t.Fatalf("payload = %+v, want zero item and copy", got)
	}
}

// Payloads survive a JSON round trip through the task table, so integers come
// back as float64 or json.Number depending on the driver.
func TestMigratePayloadSurvivesJSONRoundTrip(t *testing.T) {
	encoded, err := json.Marshal(NewMigratePayload(7, 42, 99))
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	for _, useNumber := range []bool{false, true} {
		decoded := map[string]any{}
		decoder := json.NewDecoder(strings.NewReader(string(encoded)))
		if useNumber {
			decoder.UseNumber()
		}
		if err := decoder.Decode(&decoded); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		got, err := ParseMigratePayload(&model.Task{Payload: decoded})
		if err != nil {
			t.Fatalf("ParseMigratePayload(useNumber=%v): %v", useNumber, err)
		}
		if got.ReplacementID != 7 || got.ItemID != 42 || got.CopyID != 99 {
			t.Fatalf("payload = %+v, want 7/42/99", got)
		}
	}
}

func TestMigratePayloadRejectsMissingReplacement(t *testing.T) {
	cases := map[string]*model.Task{
		"nil task":       nil,
		"nil payload":    {},
		"empty payload":  {Payload: map[string]any{}},
		"zero id":        {Payload: map[string]any{"replacement_id": 0}},
		"wrong type":     {Payload: map[string]any{"replacement_id": "seven"}},
		"only item id":   {Payload: map[string]any{"item_id": 42}},
		"negative value": {Payload: map[string]any{"replacement_id": -1}},
	}
	for name, task := range cases {
		if _, err := ParseMigratePayload(task); err == nil {
			t.Fatalf("ParseMigratePayload(%s) succeeded, want an error", name)
		}
	}
}

func TestRetirePayloadRoundTrip(t *testing.T) {
	replacementID, err := ParseRetirePayload(&model.Task{Payload: NewRetirePayload(11)})
	if err != nil {
		t.Fatalf("ParseRetirePayload: %v", err)
	}
	if replacementID != 11 {
		t.Fatalf("replacementID = %d, want 11", replacementID)
	}
	if _, err := ParseRetirePayload(&model.Task{}); err == nil {
		t.Fatal("ParseRetirePayload accepted an empty payload, want an error")
	}
}

func TestCoordinatorTaskKeysAreDistinctSingletons(t *testing.T) {
	if MigrateTaskKey(1) == MigrateTaskKey(2) {
		t.Fatal("migration keys collide across replacements")
	}
	if MigrateTaskKey(1) == RetireTaskKey(1) {
		t.Fatal("migration and retirement keys collide")
	}
	if AbandonedTargetTaskKey(1) == RetireTaskKey(1) {
		t.Fatal("abandoned-target and source retirement keys collide")
	}
	if AbandonedTargetTaskKey(1) == MigrateTaskKey(1) {
		t.Fatal("abandoned-target and migration keys collide")
	}
	for _, key := range []string{MigrateTaskKey(1), RetireTaskKey(1), AbandonedTargetTaskKey(1)} {
		if !strings.Contains(key, "storage-replacement:1:") {
			t.Fatalf("key %q does not identify replacement 1", key)
		}
	}
}

// Generic exhausted-task retry must refuse replacement work and send the
// operator back to the Data Sets surface.
func TestIsCoordinatorTask(t *testing.T) {
	migrate := StageMigrate
	retire := StageRetire
	abandoned := StageRetireAbandonedTarget
	ingress := "ingress_store"
	cases := []struct {
		name     string
		taskType model.TaskType
		stage    *string
		want     bool
	}{
		{"migration coordinator", model.TaskTypeUpload, &migrate, true},
		{"retirement coordinator", model.TaskTypeStorageCleanup, &retire, true},
		{"abandoned-target coordinator", model.TaskTypeStorageCleanup, &abandoned, true},
		{"ordinary upload", model.TaskTypeUpload, &ingress, false},
		{"no stage", model.TaskTypeUpload, nil, false},
		{"stage on the wrong task type", model.TaskTypeStorageCleanup, &migrate, false},
		{"retire stage on the wrong task type", model.TaskTypeUpload, &retire, false},
	}
	for _, tc := range cases {
		if got := IsCoordinatorTask(tc.taskType, tc.stage); got != tc.want {
			t.Fatalf("IsCoordinatorTask(%s) = %v, want %v", tc.name, got, tc.want)
		}
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
		// Wrapping is how repositories add context, so it must not lose the code.
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
