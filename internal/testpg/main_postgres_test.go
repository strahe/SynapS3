//go:build postgres

package testpg

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if err := Close(); err != nil {
		fmt.Fprintf(os.Stderr, "clean up PostgreSQL test container: %v\n", err)
		code = 1
	}
	os.Exit(code)
}
