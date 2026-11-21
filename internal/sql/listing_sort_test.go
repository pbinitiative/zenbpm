package sql

import (
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// listingQueries names the query which orders each listing of the REST API
// that offers a sortBy. The REST layer hands sortBy and sortOrder to the query
// as SortString builds them, and the query matches them in its ORDER BY.
var listingQueries = map[string]string{
	"getChildProcessInstances":       findChildProcessInstancesPage,
	"getDecisionInstances":           findDecisionInstancesPage,
	"getDmnResourceDefinitions":      findAllDmnResourceDefinitions,
	"getHistory":                     findFlowElementInstances,
	"getJobs":                        findJobs,
	"getProcessDefinitionStatistics": findProcessDefinitionStatistics,
	"getProcessDefinitions":          findProcessDefinitions,
	"getProcessInstances":            findProcessInstancesPage,
}

// TestEveryListingKnowsEverySortOfTheRestAPI shows every sortBy and sortOrder
// that openapi/api.yaml offers for a listing reaches the ORDER BY of the
// listing's query as a value it matches. A sort the query does not match falls
// through to the key, newest first, without an error, as sortBy=createdAt of
// GET /v1/jobs and sortBy=name and incidentCount of the process definitions
// did. The values come from the spec, so a sortBy added there without its
// query branch fails here.
func TestEveryListingKnowsEverySortOfTheRestAPI(t *testing.T) {
	sortsByListing := sortByEnumsOfTheRestAPI(t)

	for operationID := range listingQueries {
		require.Contains(t, sortsByListing, operationID,
			"listingQueries names %s, which openapi/api.yaml does not offer a sortBy for", operationID)
	}
	for operationID, sortsBy := range sortsByListing {
		query, ok := listingQueries[operationID]
		require.True(t, ok, "%s offers a sortBy; name the query which orders it in listingQueries", operationID)
		for _, sortBy := range sortsBy {
			for _, order := range []string{"asc", "desc"} {
				sort := SortString(&order, &sortBy)
				require.NotNil(t, sort)
				require.Contains(t, query, "WHEN '"+string(*sort)+"' ",
					"the query of %s does not order by sortBy=%s&sortOrder=%s", operationID, sortBy, order)
			}
		}
	}
}

// sortByEnumsOfTheRestAPI reads the values of the sortBy query parameter of
// every operation of openapi/api.yaml which has one, by operationId.
func sortByEnumsOfTheRestAPI(t *testing.T) map[string][]string {
	t.Helper()
	content, err := os.ReadFile("../../openapi/api.yaml")
	require.NoError(t, err)

	type parameter struct {
		Name   string `yaml:"name"`
		In     string `yaml:"in"`
		Schema struct {
			Enum []string `yaml:"enum"`
		} `yaml:"schema"`
	}
	type operation struct {
		OperationID string      `yaml:"operationId"`
		Parameters  []parameter `yaml:"parameters"`
	}
	var spec struct {
		Paths map[string]map[string]yaml.Node `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(content, &spec))

	methods := []string{"get", "put", "post", "delete", "patch"}
	sortsByListing := make(map[string][]string)
	for path, item := range spec.Paths {
		for method, node := range item {
			if !slices.Contains(methods, method) {
				continue // path-level parameters, summaries and the like
			}
			var op operation
			require.NoError(t, node.Decode(&op), "%s %s", method, path)
			for _, p := range op.Parameters {
				if p.Name != "sortBy" || p.In != "query" {
					continue
				}
				require.NotEmpty(t, p.Schema.Enum, "sortBy of %s lists no values", op.OperationID)
				sortsByListing[op.OperationID] = p.Schema.Enum
			}
		}
	}
	require.NotEmpty(t, sortsByListing, "openapi/api.yaml offers no sortBy at all; is the path right?")
	return sortsByListing
}
