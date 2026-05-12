package random

import (
	"math/rand/v2"
	"strings"
	"time"
)

const srcChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func StringN(n int) string {
	var output strings.Builder
	output.Grow(n)
	for range n {
		output.WriteByte(srcChars[rand.N(len(srcChars))])
	}
	return output.String()
}

func String() string {
	return StringN(20)
}

func Jitter(d time.Duration) time.Duration {
	return d + rand.N(d)
}
