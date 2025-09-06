// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package extensions

import "strings"

type TAssignmentDefinition struct {
	Assignee        string `xml:"assignee,attr"`
	CandidateGroups string `xml:"candidateGroups,attr"`
}

func (ad TAssignmentDefinition) GetCandidateGroups() []string {
	groups := strings.Split(ad.CandidateGroups, ",")
	for i, group := range groups {
		groups[i] = strings.TrimSpace(group)
	}
	return groups
}
