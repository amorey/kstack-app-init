// Package prefs defines the sidecar's user-preferences domain: the Settings
// shape (mirroring the cloud API) and the in-process Hub that fans Settings
// changes to subscribers. Durability lives elsewhere — the prefsync engine
// persists Settings via internal/syncstore (an Envelope carrying sync
// metadata) and publishes applied values to this package's Hub.
package prefs

// Settings mirrors the cloud API's Settings type. Today it carries a single
// free-form `placeholder` string — the POC's only synced field. Add fields
// here as the schema grows; JSON tags must match the GraphQL field names so
// the same bytes can be reused on the wire.
type Settings struct {
	Placeholder string `json:"placeholder"`
}
