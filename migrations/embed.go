// Package migrations embeds the SQL schema files so binaries can migrate on startup.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
