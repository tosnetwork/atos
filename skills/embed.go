// Package skilldoc embeds the canonical Phase 1 ATOS client skill served by
// the gateway at /skills/atos/SKILL.md.
package skilldoc

import _ "embed"

//go:embed atos/SKILL.md
var content []byte

func Content() []byte { return append([]byte(nil), content...) }
