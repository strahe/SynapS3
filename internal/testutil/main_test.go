package testutil

import (
	"testing"

	"github.com/strahe/synaps3/internal/testpg"
)

func TestMain(m *testing.M) { testpg.Main(m) }
