package admin

import (
	"testing"

	"github.com/strahe/synaps3/internal/model"
)

func TestRecurringTaskTypesHaveAdminOperations(t *testing.T) {
	defaultLabel := taskOperationLabel(model.TaskType("unknown"))
	for _, taskType := range model.RecurringSystemTaskTypes() {
		t.Run(string(taskType), func(t *testing.T) {
			if !validTaskType(taskType) {
				t.Fatalf("recurring task %s cannot be selected", taskType)
			}
			if label := taskOperationLabel(taskType); label == "" || label == defaultLabel {
				t.Fatalf("recurring task %s has no operation label", taskType)
			}
		})
	}
}
