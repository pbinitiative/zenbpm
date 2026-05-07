package dmn

import (
	"path/filepath"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestNormalizeFeelStringLiteral(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "single-line quoted string unchanged",
			input: `"hello world"`,
			want:  `"hello world"`,
		},
		{
			name: "double-line quote",
			input: `"hello
world"`,
			want: `"hello\nworld"`,
		},
		{
			name:  "already-escaped \\n in quoted string not double-escaped",
			input: `"line1\nline2"`,
			want:  `"line1\nline2"`,
		},
		{
			name:  "LF inside quoted string escaped to \\n",
			input: "\"line1\nline2\"",
			want:  `"line1\nline2"`,
		},
		{
			name:  "CRLF inside quoted string becomes single \\n",
			input: "\"line1\r\nline2\"",
			want:  `"line1\nline2"`,
		},
		{
			name:  "CR-only inside quoted string becomes \\n",
			input: "\"line1\rline2\"",
			want:  `"line1\nline2"`,
		},
		{
			name:  "multiple LF inside quoted string all escaped",
			input: "\"a\n\nb\"",
			want:  `"a\n\nb"`,
		},
		{
			name:  "whitespace-padded quoted multiline string normalized",
			input: "  \"line1\nline2\"  ",
			want:  "  \"line1\\nline2\"  ",
		},
		{
			name: "new line outside quoted string becomes \\n",
			input: `"line1"
+ "more of line1"`,
			want: `"line1"
+ "more of line1"`,
		},
		{
			name: "new line outside quoted string stays untouched",
			input: `"line1"
+ "more of line1"`,
			want: `"line1"
+ "more of line1"`,
		},
		{
			name:  "non-quoted multiline FEEL object unchanged",
			input: "{\n  a: 1,\n  b: 2\n}",
			want:  "{\n  a: 1,\n  b: 2\n}",
		},
		{
			name:  "non-quoted multiline FEEL object unchanged, but strings are escaped",
			input: "{\n  a: 1,\n  b: \"hello\nworld\"\n}",
			want:  "{\n  a: 1,\n  b: \"hello\\nworld\"\n}",
		},
		{
			name:  "non-quoted multiline FEEL arithmetic unchanged",
			input: "1 +\n2",
			want:  "1 +\n2",
		},
		{
			name:  "unquoted identifier unchanged",
			input: "myVariable",
			want:  "myVariable",
		},
		{
			name:  "new lines in quoted strings are escaped, new lines outside are not",
			input: "\"a\n\nb\" + c\n\n + \"d\n\ne\"",
			want:  "\"a\\n\\nb\" + c\n\n + \"d\\n\\ne\"",
		},
		{
			name:  "comparison operator unchanged",
			input: `"hello" = "world"`,
			want:  `"hello" = "world"`,
		},
		{
			name:  "string with an escaped quote",
			input: "\"the quote \" remains quote\"",
			want:  "\"the quote \" remains quote\"",
		},
		{
			name:  "empty string unchanged",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeFeelStringLiteral(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLiteralExpressionMultilineQuotedString(t *testing.T) {
	dmnEngine.persistence = inmemory.NewStorage()

	definition, xmldata, err := dmnEngine.ParseDmnFromFile(filepath.Join(".", "test-data", "multiline", "literal-expression.dmn"))
	assert.NoError(t, err)

	metadata, decisions, err := dmnEngine.SaveDmnResourceDefinition(nil, definition, xmldata, dmnEngine.generateKey())
	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Len(t, decisions, 1)

	result, err := dmnEngine.FindAndEvaluateDRD(nil, "latest", metadata.Id+"."+decisions[0].Id, "", nil)
	assert.NoError(t, err)
	assert.Equal(t, "Hello,\nWorld!", result.DecisionOutput)
}

func TestDecisionTableMultilineOutput(t *testing.T) {
	dmnEngine.persistence = inmemory.NewStorage()

	definition, xmldata, err := dmnEngine.ParseDmnFromFile(filepath.Join(".", "test-data", "multiline", "decision-table-output.dmn"))
	assert.NoError(t, err)

	metadata, decisions, err := dmnEngine.SaveDmnResourceDefinition(nil, definition, xmldata, dmnEngine.generateKey())
	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Len(t, decisions, 1)

	input := map[string]interface{}{"code": "A1"}
	result, err := dmnEngine.FindAndEvaluateDRD(nil, "latest", metadata.Id+"."+decisions[0].Id, "", input)
	assert.NoError(t, err)
	output, ok := result.DecisionOutput.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "Hello,\n\nthis is a multiline\nmessage.", output["message"])
}
