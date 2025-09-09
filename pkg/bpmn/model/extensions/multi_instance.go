// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package extensions

type TLoopCharacteristics struct {
	InputCollection  string `xml:"inputCollection,attr,omitempty"`
	InputElement     string `xml:"inputElement,attr"`
	OutputCollection string `xml:"outputCollection,attr"`
	OutputElement    string `xml:"outputElement,attr"`
}
