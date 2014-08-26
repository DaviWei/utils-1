package bigquery

import (
	"net/http"

	gbigquery "code.google.com/p/google-api-go-client/bigquery/v2"
)

type BigQuery struct {
	service *gbigquery.Service
}

func New(baseURL string, client *http.Client) (result *BigQuery, err error) {
	service, err := gbigquery.New(client)
	if err != nil {
		return
	}
	result = &BigQuery{
		service: service,
	}
	return
}

/*
AssertTable will check if a table named after i exists.
If it does, it will patch it so that it has all missing columns.
If it does not, it will create it.
Then it will check if there exists a view of the same table that only shows
the latest (counted by UpdatedAt) row per unique Id.
It assumes that i has a field "Id" that is a key.Key, and a field "UpdatedAt" that is a utils.Time.
*/
func (self *BigQuery) AssertTable(i interface{}) (err error) {
	projectId := "syb-core-development-warehouse"
	datasetId := "test_dataset" //"warehouse"
	table := &gbigquery.Table{}
	tablesService := gbigquery.NewTablesService(self.service)
	//list := tablesService.List(projectId, datasetId)
	notExist := false
	if notExist {
		// New empty table in dataset
		tablesService.Insert(projectId, datasetId, table)
	} else {
		tableId := ""
		tablesService.Patch(projectId, datasetId, tableId, table)
	}
	return
}
