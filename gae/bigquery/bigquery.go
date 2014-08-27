package bigquery

import (
	"fmt"
	"net/http"
	"reflect"

	gbigquery "code.google.com/p/google-api-go-client/bigquery/v2"
	"code.google.com/p/google-api-go-client/googleapi"
)

const (
	BigqueryScope  = gbigquery.BigqueryScope
	DataTypeString = "STRING"
)

type BigQuery struct {
	service   *gbigquery.Service
	projectId string
	datasetId string
}

func New(client *http.Client, projectId, datasetId string) (result *BigQuery, err error) {
	service, err := gbigquery.New(client)
	if err != nil {
		return
	}
	result = &BigQuery{
		service:   service,
		projectId: projectId,
		datasetId: datasetId,
	}
	return
}

func (self *BigQuery) createTable(val reflect.Value, tablesService *gbigquery.TablesService) (err error) {
	fmt.Println("Want to create table for", val)
	table := &gbigquery.Table{
		TableReference: &gbigquery.TableReference{
			DatasetId: self.datasetId,
			ProjectId: self.projectId,
			TableId:   val.Type().Name(),
		},
		Schema: &gbigquery.TableSchema{
			Fields: []*gbigquery.TableFieldSchema{
				&gbigquery.TableFieldSchema{
					Name: "lul",
					Type: DataTypeString,
				},
			},
		},
	}
	if _, err = tablesService.Insert(self.projectId, self.datasetId, table).Do(); err != nil {
		return
	}
	tabledataService := &gbigquery.TabledataService{}
	var response *gbigquery.TableDataInsertAllResponse
	tabledatainsertallrequest := &gbigquery.TableDataInsertAllRequest{
		Rows: []*gbigquery.TableDataInsertAllRequestRows{
			&gbigquery.TableDataInsertAllRequestRows{
				Json: map[string]gbigquery.JsonValue{
					"lul": "herp",
				},
			},
		},
	}
	if response, err = tabledataService.InsertAll(self.projectId, self.datasetId, table.TableReference.TableId, tabledatainsertallrequest).Do(); err != nil {
		return
	}
	fmt.Printf("\nresponse:%+v", response)
	return
}

func (self *BigQuery) patchTable(val reflect.Value, table *gbigquery.Table) (err error) {
	fmt.Println("Want to patch table for", val, "and", table)
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
	val := reflect.ValueOf(i)
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	tablesService := gbigquery.NewTablesService(self.service)
	table, err := tablesService.Get(self.projectId, self.datasetId, val.Type().Name()).Do()
	if err != nil {
		if gapiErr, ok := err.(*googleapi.Error); ok && gapiErr.Code == 404 {
			return self.createTable(val, tablesService)
		} else {
			return
		}
	}
	return self.patchTable(val, table)
}
