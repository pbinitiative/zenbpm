package types

import (
	"encoding/json"
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTTL(t *testing.T) {
	testCases := []struct {
		s        string
		expected TTL
	}{
		{"5ns", TTL(5 * Nanosecond)},
		{"5us", TTL(5 * Microsecond)},
		{"5µs", TTL(5 * Microsecond)},
		{"5ms", TTL(5 * Millisecond)},
		{"5s", TTL(5 * Second)},
		{"5m", TTL(5 * Minute)},
		{"5h", TTL(5 * Hour)},
		{"24h", TTL(24 * Hour)},
		{"5d", TTL(5 * Day)},
		{"5w", TTL(5 * Week)},
		{"5M", TTL(5 * Month)},
		{"5Y", TTL(5 * Year)},
		{"1w4d8h", TTL(1*Week + 4*Day + 8*Hour)},
		{"1M45m5ns", TTL(1*Month + 45*Minute + 5*Nanosecond)},
		{"1Y4m3s", TTL(1*Year + 4*Minute + 3*Second)},
		{"5s10Y", TTL(10*Year + 5*Second)},
	}

	for _, c := range testCases {
		ttl, err := ParseTTL(c.s)
		assert.NoError(t, err)
		assert.Equal(t, c.expected, ttl)
	}

}

func TestParseTTLXml(t *testing.T) {
	type TestS struct {
		XMLName xml.Name `xml:"something"`
		TTL     TTL      `xml:"ttl,attr"`
	}

	result := TestS{}

	s := `<something ttl="4d8h"/>`
	err := xml.Unmarshal([]byte(s), &result)
	assert.NoError(t, err)
	assert.Equal(t, TTL(4*Day+8*Hour), result.TTL)
}

func TestParseTTLJSON(t *testing.T) {
	type TestS struct {
		TTL TTL `json:"ttl"`
	}

	result := TestS{}

	s := `{"ttl":"4d8h"}`
	err := json.Unmarshal([]byte(s), &result)
	assert.NoError(t, err)
	assert.Equal(t, TTL(4*Day+8*Hour), result.TTL)
}

func TestParseTTLText(t *testing.T) {
	type TestS struct {
		TTL TTL `json:"ttl"`
	}

	result := TestS{}

	s := `4d8h`
	err := result.TTL.UnmarshalText([]byte(s))
	assert.NoError(t, err)
	assert.Equal(t, TTL(4*Day+8*Hour), result.TTL)
}
