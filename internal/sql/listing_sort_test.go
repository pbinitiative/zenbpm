package sql

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// sortedListing is the query which orders a listing of the REST API.
type sortedListing struct {
	query string
	// columns names what a sortBy orders by where that is not the column of the
	// same name, such as created_at for createdAt.
	columns map[string]string
}

// sortedListings names the query which orders each listing of the REST API
// that offers a sortBy. The REST layer hands sortBy and sortOrder to the query
// as SortString builds them, and the query matches them in its ORDER BY. The
// queries are listed by hand: nothing here checks that the listing runs the
// query it is listed with, so an entry is changed together with its handler.
var sortedListings = map[string]sortedListing{
	"getChildProcessInstances":  {query: findChildProcessInstancesPage},
	"getDecisionInstances":      {query: findDecisionInstancesPage, columns: map[string]string{"evaluatedAt": "created_at"}},
	"getDmnResourceDefinitions": {query: findAllDmnResourceDefinitions},
	"getHistory":                {query: findFlowElementInstances},
	"getJobs":                   {query: findJobs},
	"getProcessDefinitionStatistics": {query: findProcessDefinitionStatistics, columns: map[string]string{
		"name":          "bpmn_process_name",
		"instanceCount": "COALESCE(ic.total_instances, 0)",
		"incidentCount": "COALESCE(iic.with_incident_count, 0)",
	}},
	"getProcessDefinitions": {query: findProcessDefinitions, columns: map[string]string{"name": "bpmn_process_name"}},
	"getProcessInstances":   {query: findProcessInstancesPage},
}

// TestEveryListingKnowsEverySortOfTheRestAPI shows every sortBy and sortOrder
// that openapi/api.yaml offers for a listing is matched in the ORDER BY of the
// query sortedListings names for it, and orders by the column it stands for in
// the direction it asks for. A sort the query does not match falls through to
// the order of the query without a sort, without an error, as
// sortBy=createdAt of GET /v1/jobs and sortBy=name and incidentCount of the
// process definitions did. The values come from the spec, so a sortBy added
// there without its query branch fails here. Every sort a query misses is
// reported, not only the first.
func TestEveryListingKnowsEverySortOfTheRestAPI(t *testing.T) {
	sortsByListing := sortByEnumsOfTheRestAPI(t)

	for operationID := range sortedListings {
		assert.Contains(t, sortsByListing, operationID,
			"sortedListings names %s, which openapi/api.yaml does not offer a sortBy for", operationID)
	}
	for operationID, sortsBy := range sortsByListing {
		listing, ok := sortedListings[operationID]
		if !assert.True(t, ok, "%s offers a sortBy; name the query which orders it in sortedListings", operationID) {
			continue
		}
		for sortBy := range listing.columns {
			assert.Contains(t, sortsBy, sortBy, "sortedListings names a column for sortBy=%s of %s, which does not offer it", sortBy, operationID)
		}
		for _, sortBy := range sortsBy {
			column, ok := listing.columns[sortBy]
			if !ok {
				column = snakeCase(sortBy)
			}
			for _, order := range []string{"asc", "desc"} {
				sort := SortString(&order, &sortBy)
				require.NotNil(t, sort)
				orderings := orderingsOf(listing.query, string(*sort))
				assert.NotEmpty(t, orderings, "the query of %s does not order by sortBy=%s&sortOrder=%s", operationID, sortBy, order)
				// a query which pages in a subquery orders there and once more outside
				for _, ordering := range orderings {
					assert.Equal(t, column+" "+strings.ToUpper(order), ordering,
						"what the query of %s orders by for sortBy=%s&sortOrder=%s", operationID, sortBy, order)
				}
			}
		}
	}
}

// orderingsOf lists what the query orders by for a sort, as "<column>
// <direction>", once for every ORDER BY which matches the sort. A plain column
// is named without its table and quotes, an expression as the query has it.
func orderingsOf(query, sort string) []string {
	when := regexp.MustCompile(`WHEN '` + regexp.QuoteMeta(sort) + `'\s+THEN\s+(.+?)\s+END\s+(ASC|DESC)\b`)
	plainColumn := regexp.MustCompile(`^(?:\w+\.)?"?(\w+)"?$`)
	var orderings []string
	for _, match := range when.FindAllStringSubmatch(query, -1) {
		orderings = append(orderings, plainColumn.ReplaceAllString(match[1], "$1")+" "+match[2])
	}
	return orderings
}

// snakeCase is the column of a sortBy which is named like it: created_at for
// createdAt.
func snakeCase(sortBy string) string {
	return strings.ToLower(regexp.MustCompile(`[A-Z]`).ReplaceAllString(sortBy, "_$0"))
}

// sortByEnumsOfTheRestAPI reads the values of the sortBy query parameter of
// every operation of openapi/api.yaml which has one, by operationId. A
// parameter an operation refers to in the spec's components counts as its own,
// and so does one of its path, unless the operation defines it anew.
func sortByEnumsOfTheRestAPI(t *testing.T) map[string][]string {
	t.Helper()
	content, err := os.ReadFile("../../openapi/api.yaml")
	require.NoError(t, err)

	type parameter struct {
		Ref    string `yaml:"$ref"`
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
		Paths      map[string]map[string]yaml.Node `yaml:"paths"`
		Components struct {
			Parameters map[string]parameter `yaml:"parameters"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal(content, &spec))

	methods := []string{"get", "put", "post", "delete", "patch"}
	sortsByListing := make(map[string][]string)
	for path, item := range spec.Paths {
		for method, node := range item {
			if !slices.Contains(methods, method) {
				continue // summaries, servers and the like; parameters are read with each operation
			}
			var op operation
			require.NoError(t, node.Decode(&op), "%s %s", method, path)
			var pathParameters []parameter
			if node, ok := item["parameters"]; ok {
				require.NoError(t, node.Decode(&pathParameters), "parameters of %s", path)
			}
			// the operation's own parameters come last, so they win
			for _, p := range slices.Concat(pathParameters, op.Parameters) {
				if p.Ref != "" {
					name, ok := strings.CutPrefix(p.Ref, "#/components/parameters/")
					require.True(t, ok, "%s refers to the parameter %s, which is not among the spec's parameters", op.OperationID, p.Ref)
					p, ok = spec.Components.Parameters[name]
					require.True(t, ok, "%s refers to the parameter %s, which the spec does not define", op.OperationID, name)
				}
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
