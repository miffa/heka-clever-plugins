package heka_clever_plugins

import (
	gs "github.com/rafrombrc/gospec/src/gospec"
	"testing"
)

func TestAllSpecs(t *testing.T) {
	universalT = t
	r := gs.NewRunner()
	r.Parallel = false

	r.AddSpec(KeenOutputSpec)
	r.AddSpec(JsonDecoderSpec)
	r.AddSpec(JsonEncoderSpec)

	gs.MainGoTest(r, t)
}
