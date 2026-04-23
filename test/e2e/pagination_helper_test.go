package e2e

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TotalCountMode controls whether the totalCount assertion is exact or a lower bound.
type TotalCountMode int

const (
	ExactCount TotalCountMode = iota
	AtLeastCount
)

type PageExpectation struct {
	Page          int
	ExpectedCount int
}

type PageScenario struct {
	PageSize       int
	Pages          []PageExpectation
	TotalCount     int
	TotalCountMode TotalCountMode
}

type PaginationTestConfig struct {
	EndpointName string

	Setup func(t *testing.T) (cleanup func())

	FetchPage func(t *testing.T, page, size int) (returnedCount int, totalCount int, receivedPage int, receivedSize int)

	Scenarios []PageScenario
}

func RunPaginationTests(t *testing.T, cfg PaginationTestConfig) {
	t.Helper()

	if cfg.Setup != nil {
		cleanup := cfg.Setup(t)
		if cleanup != nil {
			t.Cleanup(cleanup)
		}
	}

	for _, scenario := range cfg.Scenarios {
		scenario := scenario
		t.Run(fmt.Sprintf("%s/size=%d", cfg.EndpointName, scenario.PageSize), func(t *testing.T) {
			for _, page := range scenario.Pages {
				page := page
				t.Run(fmt.Sprintf("page=%d", page.Page), func(t *testing.T) {
					t.Parallel()
					returnedCount, totalCount, receivedPage, receivedSize := cfg.FetchPage(t, page.Page, scenario.PageSize)

					assert.Equal(t, page.Page, receivedPage,
						"received page number %d should be the same as page %d from request",
						receivedPage, page.Page)

					assert.Equal(t, scenario.PageSize, receivedSize,
						"received page size %d should be the same as page size %d from request",
						receivedSize, scenario.PageSize)

					assert.Equal(t, page.ExpectedCount, returnedCount,
						"page %d with size %d should return %d items",
						page.Page, scenario.PageSize, page.ExpectedCount)

					switch scenario.TotalCountMode {
					case ExactCount:
						assert.Equal(t, scenario.TotalCount, totalCount,
							"totalCount should be exactly %d", scenario.TotalCount)
					case AtLeastCount:
						assert.GreaterOrEqual(t, totalCount, scenario.TotalCount,
							"totalCount should be at least %d", scenario.TotalCount)
					}
				})
			}
		})
	}
}
