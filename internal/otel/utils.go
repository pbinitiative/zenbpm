// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package otel

import (
	"go.opentelemetry.io/otel/attribute"
)

const (
	ReadBytesKey  = attribute.Key("http.read_bytes")  // if anything was read from the request body, the total number of bytes read
	ReadErrorKey  = attribute.Key("http.read_error")  // If an error occurred while reading a request, the string of the error (io.EOF is not recorded)
	WroteBytesKey = attribute.Key("http.wrote_bytes") // if anything was written to the response writer, the total number of bytes written
	WriteErrorKey = attribute.Key("http.write_error") // if an error occurred while writing a reply, the string of the error (io.EOF is not recorded)

	AttributeQuery = "rqlite-query"
	AttributeExec  = "rqlite-exec"
	AttributeArgs  = "rqlite-args"
)

// used by middleware to create context key for configured transfer headers
type TransferHeaderKey string
