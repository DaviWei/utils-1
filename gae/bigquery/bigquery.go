package bigquery

import (
	"fmt"
	"net/http"
	"reflect"

	gbigquery "code.google.com/p/google-api-go-client/bigquery/v2"
	"code.google.com/p/google-api-go-client/googleapi"
)

const (
	BigqueryScope     = gbigquery.BigqueryScope
	dataTypeString    = "STRING"
	dataTypeInteger   = "INTEGER"
	dataTypeRecord    = "RECORD"
	dataTypeFloat     = "FLOAT"
	dataTypeBool      = "BOOLEAN"
	dataTypeTimeStamp = "STRING" // "TIMESTAMP"
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

// If not found,  invalid to insert in bigquery
var biqqueryDataTypes = map[reflect.Kind]string{
	reflect.Bool:   dataTypeBool,
	reflect.Int:    dataTypeInteger,
	reflect.Int8:   dataTypeInteger,
	reflect.Int16:  dataTypeInteger,
	reflect.Int32:  dataTypeInteger,
	reflect.Int64:  dataTypeInteger,
	reflect.Uint:   dataTypeInteger,
	reflect.Uint8:  dataTypeInteger,
	reflect.Uint16: dataTypeInteger,
	reflect.Uint32: dataTypeInteger,
	reflect.Uint64: dataTypeInteger,
	//reflect.Uintptr: dataTypeRecord,
	reflect.Float32: dataTypeFloat,
	reflect.Float64: dataTypeFloat,
	//reflect.Array:   dataTypeRecord,

	//reflect.Map: dataTypeRecord,
	reflect.Ptr: dataTypeRecord,
	//reflect.Slice:  dataTypeRecord,
	reflect.String: dataTypeString,
	reflect.Struct: dataTypeRecord,
}

func buildSchemaFields(val reflect.Value) (result []*gbigquery.TableFieldSchema) {
	var schemaFields []*gbigquery.TableFieldSchema

	for i := 0; i < val.Type().NumField(); i++ {
		field := val.Type().Field(i)

		schemaField := &gbigquery.TableFieldSchema{}

		found := false
		if schemaField.Type, found = biqqueryDataTypes[field.Type.Kind()]; !found {
			fmt.Printf("\nReflect kind %v (field name: %v) not supported.\n", field.Type.Kind(), field.Name)
			return
		}
		schemaField.Name = field.Name

		if schemaField.Type == dataTypeRecord {
			fieldVal := val.Field(i)
			for fieldVal.Kind() == reflect.Ptr {
				schemaField.Description = "This field was originally a pointer."
				fieldVal = val.Field(i).Elem()
			}
			if fieldVal.Kind() == reflect.Invalid {
				// If we end up here, schema is never built??
				fmt.Printf("\nInvalid kind on value.\n")
				return
			}
			schemaField.Fields = buildSchemaFields(fieldVal)
		}

		schemaFields = append(schemaFields, schemaField)
	}

	result = schemaFields
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
			Fields: buildSchemaFields(val),
		},
	}
	if _, err = tablesService.Insert(self.projectId, self.datasetId, table).Do(); err != nil {
		return
	}
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
