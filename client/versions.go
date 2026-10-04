package client

import "slices"

// supportedProtocolVersions are the protocol versions this client speaks,
// newest first. Supporting a new version means adding it here, and adding a
// check below for anything it changes that this client uses.
var supportedProtocolVersions = []string{
	"2026-07-28",
	"2025-11-25",
	"2025-06-18",
	"2025-03-26",
	"2024-11-05",
}

// statelessSince is the version that made the protocol stateless: the client
// asks what the server supports with server/discover instead of the
// initialize handshake, every request carries the request metadata, and
// notifications come through subscriptions/listen. Protocol versions are dates,
// so they compare as strings.
const statelessSince = "2026-07-28"

// isStateless reports whether a version is one of the stateless protocol.
func isStateless(version string) bool {
	return version >= statelessSince
}

// statelessVersions and handshakeVersions split supportedProtocolVersions by
// how a session in that version starts, each newest first.
var (
	statelessVersions = slices.DeleteFunc(slices.Clone(supportedProtocolVersions), func(v string) bool { return !isStateless(v) })
	handshakeVersions = slices.DeleteFunc(slices.Clone(supportedProtocolVersions), isStateless)
)

// newestShared is the newest of versions that the other side also offers, or
// "" when they share none.
func newestShared(versions, offered []string) string {
	for _, v := range versions {
		if slices.Contains(offered, v) {
			return v
		}
	}
	return ""
}
