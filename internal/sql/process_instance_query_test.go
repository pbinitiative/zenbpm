package sql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindProcessInstancesPageCountsIncidentsAfterPagination(t *testing.T) {
	requireCountsIncidentsAfterPagination(t, findProcessInstancesPage)
}

func TestFindChildProcessInstancesPageCountsIncidentsAfterPagination(t *testing.T) {
	requireCountsIncidentsAfterPagination(t, findChildProcessInstancesPage)
}

func requireCountsIncidentsAfterPagination(t *testing.T, query string) {
	t.Helper()

	paginationPosition := strings.Index(query, "\nLIMIT ")
	incidentCountPosition := strings.Index(query, "SELECT COUNT(*)\n        FROM incident AS i")

	require.GreaterOrEqual(t, paginationPosition, 0, "query should paginate the filtered process instances")
	require.Greater(t, incidentCountPosition, paginationPosition, "incident count should run after pagination")
	require.Contains(t, query, "WHERE i.process_instance_key = paged.key")
}
