// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package e2e

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

func resolveIncident(t testing.TB, key int64) error {
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/incidents/%d/resolve", key)).
		WithMethod("POST").
		DoOk()
	if err != nil {
		return fmt.Errorf("failed to resolve incident: %w", err)
	}
	response := public.ResolveIncident201Response{}
	err = json.Unmarshal(resp, &response)
	if err != nil {
		return fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return nil
}
