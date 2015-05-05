package config

import (
	mesos "github.com/channelmeter/caddy-plugin-mesos"
	"github.com/mholt/caddy/config/parse"
)

func init() {
	insertAt := func(idx int) {
		directiveOrder = append(directiveOrder, directiveOrder[len(directiveOrder)-1])
		copy(directiveOrder[idx+1:], directiveOrder[idx:])
		directiveOrder[idx] = directive{"mesos", mesos.Proxy}
	}
	inserted := false
	for i, directive := range directiveOrder {
		if directive.name == "proxy" {
			insertAt(i + 1)
			inserted = true
			break
		}
	}
	if !inserted {
		insertAt(len(directiveOrder))
	}
	parse.ValidDirectives["mesos"] = struct{}{}
}
