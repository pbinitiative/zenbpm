package extensions

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/senseyeio/duration"
)

// IsExpression reports whether an attribute value is a FEEL expression, which
// starts with '='. Such a value is evaluated when a job is created, not parsed
// when the definition is deployed.
func IsExpression(value string) bool {
	return strings.HasPrefix(strings.TrimSpace(value), "=")
}

// ParseRetries reads a literal retries attribute: a non-negative integer.
func ParseRetries(literal string) (int32, error) {
	trimmed := strings.TrimSpace(literal)
	retries, err := strconv.ParseInt(trimmed, 10, 32)
	if errors.Is(err, strconv.ErrRange) && !strings.HasPrefix(trimmed, "-") {
		return 0, fmt.Errorf("retries %q is larger than %d", literal, math.MaxInt32)
	}
	if errors.Is(err, strconv.ErrRange) {
		return 0, fmt.Errorf("retries %q must not be negative", literal)
	}
	if err != nil {
		return 0, fmt.Errorf("retries %q is not an integer", literal)
	}
	if retries < 0 {
		return 0, fmt.Errorf("retries %q must not be negative", literal)
	}
	return int32(retries), nil
}

// ParseRetryBackoff reads a backoff policy: one ISO-8601 duration ("PT10S") or
// a comma-separated list of them ("PT10S,PT1M,PT10M"), the n-th failure of a
// job waiting the n-th entry and the last entry repeating.
func ParseRetryBackoff(policy string) ([]time.Duration, error) {
	entries := strings.Split(policy, ",")
	backoffs := make([]time.Duration, 0, len(entries))
	for _, entry := range entries {
		backoff, err := ParseBackoffDuration(entry)
		if err != nil {
			return nil, fmt.Errorf("retryBackoff %q: %w", policy, err)
		}
		backoffs = append(backoffs, backoff)
	}
	return backoffs, nil
}

// ParseBackoffDuration reads one ISO-8601 duration of a backoff. Years and
// months are refused because their length depends on the date; a day counts
// 24 hours and a week seven days. A duration beyond what time.Duration holds
// saturates at its maximum, which the engine's cap then lowers.
func ParseBackoffDuration(value string) (time.Duration, error) {
	trimmed := strings.TrimSpace(value)
	parsed, err := duration.ParseISO8601(trimmed)
	if err != nil || trimmed == "P" || strings.HasSuffix(trimmed, "T") {
		return 0, fmt.Errorf("%q is not an ISO-8601 duration such as PT10S", value)
	}
	if parsed.Y != 0 || parsed.M != 0 {
		return 0, fmt.Errorf("%q uses years or months, whose length depends on the date: use weeks, days, hours, minutes or seconds", value)
	}
	return FixedLengthOf(parsed), nil
}

// FixedLengthOf converts the weeks, days, hours, minutes and seconds of an
// ISO-8601 duration into a time.Duration, a week counting seven days and a day
// 24 hours, saturating at the maximum instead of wrapping. Years and months,
// whose length depends on the date, are left out.
func FixedLengthOf(parsed duration.Duration) time.Duration {
	const maxSeconds = math.MaxInt64 / int64(time.Second)
	seconds := int64(0)
	for _, part := range []struct{ count, secondsPerUnit int64 }{
		{int64(parsed.W), 7 * 24 * 60 * 60},
		{int64(parsed.D), 24 * 60 * 60},
		{int64(parsed.TH), 60 * 60},
		{int64(parsed.TM), 60},
		{int64(parsed.TS), 1},
	} {
		if part.count > (maxSeconds-seconds)/part.secondsPerUnit {
			return time.Duration(math.MaxInt64)
		}
		seconds += part.count * part.secondsPerUnit
	}
	return time.Duration(seconds) * time.Second
}

// FormatRetryBackoff writes a backoff policy the way ParseRetryBackoff reads
// it, each entry in whole seconds ("PT10S,PT1M"). An empty policy is "".
func FormatRetryBackoff(policy []time.Duration) string {
	entries := make([]string, len(policy))
	for i, backoff := range policy {
		entries[i] = FormatBackoffDuration(backoff)
	}
	return strings.Join(entries, ",")
}

// FormatBackoffDuration writes a duration as ISO-8601 hours, minutes and whole
// seconds; zero is "PT0S".
func FormatBackoffDuration(backoff time.Duration) string {
	seconds := int64(backoff / time.Second)
	if seconds <= 0 {
		return "PT0S"
	}
	formatted := []byte("PT")
	if hours := seconds / 3600; hours > 0 {
		formatted = append(strconv.AppendInt(formatted, hours, 10), 'H')
	}
	if minutes := seconds % 3600 / 60; minutes > 0 {
		formatted = append(strconv.AppendInt(formatted, minutes, 10), 'M')
	}
	if rest := seconds % 60; rest > 0 {
		formatted = append(strconv.AppendInt(formatted, rest, 10), 'S')
	}
	return string(formatted)
}
