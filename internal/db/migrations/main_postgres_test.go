//go:build postgres

package migrations

import (
	"fmt"
	"os"
	"testing"

	"github.com/strahe/synaps3/internal/testpg"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if err := testpg.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "clean up PostgreSQL test container: %v\n", err)
		code = 1
	}
	os.Exit(code)
}
