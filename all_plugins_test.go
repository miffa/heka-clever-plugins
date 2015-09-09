package heka_clever_plugins

import (
	"testing"

	gs "github.com/rafrombrc/gospec/src/gospec"
)

func TestAllSpecs(t *testing.T) {
	universalT = t
	r := gs.NewRunner()
	r.Parallel = false

	r.AddSpec(KeenOutputSpec)
	r.AddSpec(JsonDecoderSpec)

	gs.MainGoTest(r, t)
}
