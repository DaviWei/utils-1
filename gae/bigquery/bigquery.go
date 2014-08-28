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

func getDataType(field reflect.StructField) (dataType string) {
	switch field.Type.Kind() {
	// Strings
	case reflect.String:
		dataType = dataTypeString

		// Integers
	case reflect.Int:
		dataType = dataTypeInteger
	case reflect.Int32:
		dataType = dataTypeInteger
	case reflect.Int64:
		dataType = dataTypeInteger

		// Nested structs - recursion?
	case reflect.Struct:
		dataType = dataTypeRecord

		// Floats
	case reflect.Float32:
		dataType = dataTypeFloat
	case reflect.Float64:
		dataType = dataTypeFloat

		// Bools
	case reflect.Bool:
		dataType = dataTypeBool

		/*case reflect.TimeStamp:
		dataType = dataTypeTimeStamo
		*/

		// Pointers, most likely structs but not neccesarily
	case reflect.Ptr:
		//getDataType(field.Type.Elem())
		dataType = dataTypeRecord

	default:
		panic(fmt.Errorf("field:%v", field.Type.Kind()))
	}

	return
}

func buildSchemaFields(val reflect.Value) (result []*gbigquery.TableFieldSchema) {
	var schemaFields []*gbigquery.TableFieldSchema

	fmt.Printf("\n\nval.Type():%v\n\n", val.Type())

	for i := 0; i < val.Type().NumField(); i++ {
		field := val.Type().Field(i)
		dataType := getDataType(field)

		schemaField := &gbigquery.TableFieldSchema{}

		fmt.Printf("\n\ndatatype:%v\n\n", dataType)

		if dataType == dataTypeRecord {
			// TODO: Handle not found structs
			nestedStruct, _ := field.Type.Elem().FieldByName(field.Name)
			schemaField.Fields = buildSchemaFields(reflect.ValueOf(nestedStruct))
		}

		schemaField.Name = field.Name
		schemaField.Type = dataType

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
