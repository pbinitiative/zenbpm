package rsync

import (
	"fmt"
	"time"
)

type PollTrue struct {
	fn           func() bool
	pollInterval time.Duration
	timeout      time.Duration
}

func NewPollTrue(fn func() bool, pollInterval time.Duration, timeout time.Duration) *PollTrue {
	return &PollTrue{
		fn:           fn,
		pollInterval: pollInterval,
		timeout:      timeout,
	}
}

func (rt *PollTrue) Run(name string) error {
	if rt.fn() {
		return nil
	}
	timer := time.NewTimer(rt.timeout)
	defer timer.Stop()
	ticker := time.NewTicker(rt.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timeout waiting for %s", name)
		case <-ticker.C:
			if rt.fn() {
				return nil
			}
		}
	}
}
