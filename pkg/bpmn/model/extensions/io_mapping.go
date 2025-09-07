// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package extensions

//TODO: This needs to be revised licensewise

type TIoMapping struct {
	Source string `xml:"source,attr"`
	Target string `xml:"target,attr"`
}

type TCalledElement struct {
	ProcessId   string  `xml:"processId,attr"`
	BindingType *string `xml:"bindingType,attr,omitempty"`
	VersionTag  *string `xml:"versionTag,attr,omitempty"`
}
