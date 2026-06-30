module github.com/mtgo-labs/plugins/rawdebug

go 1.26.2

require github.com/mtgo-labs/mtgo v0.11.0

require (
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/mtgo-labs/storage v0.4.1 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/term v0.44.0 // indirect
)

// Build against the local mtgo working copy until the update/transport
// hooks (OnUpdateReceived, OnConnected, OnReconnect) are released.
// Remove this replace and bump the require when the next mtgo version
// (>= v0.11.1) ships those hooks.
replace github.com/mtgo-labs/mtgo => ../../mtgo
