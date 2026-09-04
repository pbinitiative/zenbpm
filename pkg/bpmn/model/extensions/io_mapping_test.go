package extensions

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalledElementResolveVersion(t *testing.T) {
	int32Pointer := func(value int32) *int32 { return &value }
	stringPointer := func(value string) *string { return &value }

	tests := []struct {
		name            string
		calledElement   TCalledElement
		expectedVersion *int32
		expectedTag     string
		expectedError   string
	}{
		{name: "missing binding uses latest"},
		{
			name:          "missing binding ignores stale version tag",
			calledElement: TCalledElement{VersionTag: stringPointer("v1")},
		},
		{
			name:            "direct numeric version",
			calledElement:   TCalledElement{Version: int32Pointer(2)},
			expectedVersion: int32Pointer(2),
		},
		{
			name:          "version tag binding passes tag through",
			calledElement: TCalledElement{BindingType: stringPointer("versionTag"), VersionTag: stringPointer("stable-1")},
			expectedTag:   "stable-1",
		},
		{
			name:          "latest binding ignores stale version tag",
			calledElement: TCalledElement{BindingType: stringPointer("latest"), VersionTag: stringPointer("v1")},
		},
		{
			name:          "deployment binding preserves legacy latest behavior",
			calledElement: TCalledElement{BindingType: stringPointer("deployment")},
		},
		{
			name:          "unknown binding preserves legacy latest behavior",
			calledElement: TCalledElement{BindingType: stringPointer("other")},
		},
		{
			name:          "numeric and version tag conflict",
			calledElement: TCalledElement{Version: int32Pointer(1), BindingType: stringPointer("versionTag"), VersionTag: stringPointer("v1")},
			expectedError: "cannot configure both numeric version and versionTag",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			selection, err := test.calledElement.ResolveVersion()
			if test.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.expectedError)
				assert.Equal(t, VersionSelection{}, selection)
				return
			}
			require.NoError(t, err)
			if test.expectedVersion == nil {
				assert.Nil(t, selection.Version)
			} else {
				require.NotNil(t, selection.Version)
				assert.Equal(t, *test.expectedVersion, *selection.Version)
			}
			assert.Equal(t, test.expectedTag, selection.VersionTag)
		})
	}
}

func TestCalledElementResolveVersionRejectsInvalidVersionTags(t *testing.T) {
	bindingType := "versionTag"
	cases := []struct {
		tag   string
		error string
	}{
		{tag: "", error: `bindingType "versionTag" requires versionTag`},
	}
	for _, c := range cases {
		t.Run(c.tag, func(t *testing.T) {
			tag := c.tag
			calledElement := TCalledElement{BindingType: &bindingType, VersionTag: &tag}
			selection, err := calledElement.ResolveVersion()
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.error)
			assert.Equal(t, VersionSelection{}, selection)
		})
	}
}
