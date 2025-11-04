package types

import (
	"encoding/xml"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// TTL is expanded time.Duration from standard library
// TODO: maybe switch to iso duration?
type TTL time.Duration

func (c TTL) Seconds() float64 {
	return time.Duration(c).Seconds()
}

const (
	Nanosecond  time.Duration = 1
	Microsecond               = 1000 * Nanosecond
	Millisecond               = 1000 * Microsecond
	Second                    = 1000 * Millisecond
	Minute                    = 60 * Second
	Hour                      = 60 * Minute
	Day                       = 24 * Hour
	Week                      = 7 * Day
	Month                     = 30 * Day
	Year                      = 365 * Day
)

var unitMap = map[string]uint64{
	"ns": uint64(Nanosecond),
	"us": uint64(Microsecond),
	"µs": uint64(Microsecond), // U+00B5 = micro symbol
	"μs": uint64(Microsecond), // U+03BC = Greek letter mu
	"ms": uint64(Millisecond),
	"s":  uint64(Second),
	"m":  uint64(Minute),
	"h":  uint64(Hour),
	"d":  uint64(Day),
	"w":  uint64(Week),
	"M":  uint64(Month),
	"Y":  uint64(Year),
}

func (c *TTL) UnmarshalXMLAttr(attr xml.Attr) error {
	ttl, err := ParseTTL(attr.Value)
	if err != nil {
		return fmt.Errorf("failed to parse TTL string: %w", err)
	}
	*c = ttl
	return nil
}

func (c *TTL) UnmarshalJSON(s []byte) error {
	str, err := strconv.Unquote(string(s))
	if err != nil {
		return fmt.Errorf("failed to unquote TTL string: %w", err)
	}
	ttl, err := ParseTTL(str)
	if err != nil {
		return fmt.Errorf("failed to parse TTL string: %w", err)
	}
	*c = ttl
	return nil
}

func (c *TTL) UnmarshalText(s []byte) error {
	ttl, err := ParseTTL(string(s))
	if err != nil {
		return fmt.Errorf("failed to parse TTL string: %w", err)
	}
	*c = ttl
	return nil
}

func ParseTTL(s string) (TTL, error) {
	// [-+]?([0-9]*(\.[0-9]*)?[a-z]+)+
	orig := s
	var ttl uint64
	neg := false

	// Consume [-+]?
	if s != "" {
		c := s[0]
		if c == '-' || c == '+' {
			neg = c == '-'
			s = s[1:]
		}
	}
	// Special case: if all that is left is "0", this is zero.
	if s == "0" {
		return 0, nil
	}
	if s == "" {
		return 0, fmt.Errorf("time: invalid TTL \"%s\"", orig)
	}
	for s != "" {
		var (
			v, f  uint64      // integers before, after decimal point
			scale float64 = 1 // value = v + f/scale
		)

		var err error

		// The next character must be [0-9.]
		if !(s[0] == '.' || '0' <= s[0] && s[0] <= '9') {
			return 0, fmt.Errorf("time: invalid TTL \"%s\" ", orig)
		}
		// Consume [0-9]*
		pl := len(s)
		v, s, err = leadingInt(s)
		if err != nil {
			return 0, fmt.Errorf("time: invalid TTL \"%s\"", orig)
		}
		pre := pl != len(s) // whether we consumed anything before a period

		// Consume (\.[0-9]*)?
		post := false
		if s != "" && s[0] == '.' {
			s = s[1:]
			pl := len(s)
			f, scale, s = leadingFraction(s)
			post = pl != len(s)
		}
		if !pre && !post {
			// no digits (e.g. ".s" or "-.s")
			return 0, fmt.Errorf("time: invalid TTL \"%s\"", orig)
		}

		// Consume unit.
		i := 0
		for ; i < len(s); i++ {
			c := s[i]
			if c == '.' || '0' <= c && c <= '9' {
				break
			}
		}
		if i == 0 {
			return 0, fmt.Errorf("time: missing unit in TTL \"%s\"", orig)
		}
		u := s[:i]
		s = s[i:]
		unit, ok := unitMap[u]
		if !ok {
			return 0, fmt.Errorf("time: unknown unit \"%s\" in TTL \"%s\"", u, orig)
		}
		if v > 1<<63/unit {
			// overflow
			return 0, fmt.Errorf("time: invalid TTL \"%s\"", orig)
		}
		v *= unit
		if f > 0 {
			// float64 is needed to be nanosecond accurate for fractions of hours.
			// v >= 0 && (f*unit/scale) <= 3.6e+12 (ns/h, h is the largest unit)
			v += uint64(float64(f) * (float64(unit) / scale))
			if v > 1<<63 {
				// overflow
				return 0, fmt.Errorf("time: invalid TTL \"%s\"", orig)
			}
		}
		ttl += v
		if ttl > 1<<63 {
			return 0, fmt.Errorf("time: invalid TTL \"%s\"", orig)
		}
	}
	if neg {
		return TTL(-ttl), nil
	}
	if ttl > 1<<63-1 {
		return 0, fmt.Errorf("time: invalid TTL \"%s\"", orig)
	}
	return TTL(ttl), nil
}

var errLeadingInt = errors.New("time: bad [0-9]*") // never printed

// leadingInt consumes the leading [0-9]* from s.
func leadingInt[bytes []byte | string](s bytes) (x uint64, rem bytes, err error) {
	i := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if x > 1<<63/10 {
			// overflow
			return 0, rem, errLeadingInt
		}
		x = x*10 + uint64(c) - '0'
		if x > 1<<63 {
			// overflow
			return 0, rem, errLeadingInt
		}
	}
	return x, s[i:], nil
}

// leadingFraction consumes the leading [0-9]* from s.
// It is used only for fractions, so does not return an error on overflow,
// it just stops accumulating precision.
func leadingFraction(s string) (x uint64, scale float64, rem string) {
	i := 0
	scale = 1
	overflow := false
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if overflow {
			continue
		}
		if x > (1<<63-1)/10 {
			// It's possible for overflow to give a positive number, so take care.
			overflow = true
			continue
		}
		y := x*10 + uint64(c) - '0'
		if y > 1<<63 {
			overflow = true
			continue
		}
		x = y
		scale *= 10
	}
	return x, scale, s[i:]
}
