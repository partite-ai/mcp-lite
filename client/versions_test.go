package client

import "testing"

// A version newer than any this client knows is still placed by the features
// it inherits, so adding it to supportedProtocolVersions is all it takes.
func TestVersionsArePlacedByTheFeaturesTheyHave(t *testing.T) {
	cases := map[string]bool{
		"2024-11-05": false,
		"2025-11-25": false,
		"2026-07-28": true,
		"2027-03-01": true,
	}
	for version, want := range cases {
		if got := isStateless(version); got != want {
			t.Errorf("isStateless(%q) = %v; want %v", version, got, want)
		}
	}
}

func TestNewestSharedPrefersTheNewestOfOurs(t *testing.T) {
	ours := []string{"2027-03-01", "2026-07-28", "2025-11-25"}
	if got := newestShared(ours, []string{"2025-11-25", "2026-07-28"}); got != "2026-07-28" {
		t.Errorf("newestShared = %q; want 2026-07-28", got)
	}
	if got := newestShared(ours, []string{"2024-11-05"}); got != "" {
		t.Errorf("newestShared = %q; want none", got)
	}
}

// The handshake starts from the newest version of each kind, so each kind
// needs at least one.
func TestEachKindOfVersionHasOne(t *testing.T) {
	if len(statelessVersions) == 0 || len(handshakeVersions) == 0 {
		t.Fatalf("stateless versions %v, handshake versions %v; want at least one of each", statelessVersions, handshakeVersions)
	}
}
