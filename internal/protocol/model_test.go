package protocol

import "testing"

func TestWireEnumValues(t *testing.T) {
	checks := map[string]struct{ got, want int32 }{
		"command sync": {int32(CommandSync), 0}, "command provision": {int32(CommandProvision), 3},
		"namespace statistic": {int32(NamespaceStatistic), 0}, "namespace chat": {int32(NamespaceChat), 1}, "namespace notify": {int32(NamespaceNotify), 5},
		"route online": {int32(RouteOnline), 1}, "route direct": {int32(RouteDirect), 2},
		"status redirect": {int32(StatusRedirect), 5}, "status resource changed": {int32(StatusResourceChanged), 20},
		"content video": {int32(ContentVideo), 2}, "content voice": {int32(ContentVoice), 4}, "content custom": {int32(ContentCustom), 7},
	}
	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("%s=%d want %d", name, c.got, c.want)
		}
	}
}
