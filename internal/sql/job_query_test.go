package sql

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/require"
)

// TestFindJobsKnowsEverySortOfTheRestApi shows every sort GET /v1/jobs offers
// reaches the ORDER BY of FindJobs as a value it matches. A sort it does not
// match falls through to the key, newest first, without an error, as sortBy
// createdAt did while the query spelled it created_at.
func TestFindJobsKnowsEverySortOfTheRestApi(t *testing.T) {
	sortsBy := []public.GetJobsParamsSortBy{
		public.GetJobsParamsSortByCreatedAt,
		public.GetJobsParamsSortByKey,
		public.GetJobsParamsSortByState,
		public.GetJobsParamsSortByType,
	}
	orders := []public.GetJobsParamsSortOrder{
		public.GetJobsParamsSortOrderAsc,
		public.GetJobsParamsSortOrderDesc,
	}
	for _, sortBy := range sortsBy {
		for _, order := range orders {
			sort := SortString(&order, &sortBy)
			require.NotNil(t, sort)
			require.Contains(t, findJobs, "WHEN '"+string(*sort)+"' ",
				"FindJobs does not order by sortBy=%s&sortOrder=%s", sortBy, order)
		}
	}
}
