package bigquery

import (
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/soundtrackyourbrand/utils"

	gbigquery "code.google.com/p/google-api-go-client/bigquery/v2"
	"code.google.com/p/google-api-go-client/googleapi"
)

var timeType = reflect.TypeOf(time.Now())
var byteStringType = reflect.TypeOf(utils.ByteString{[]byte{0}})

const (
	BigqueryScope     = gbigquery.BigqueryScope
	dataTypeString    = "STRING"
	dataTypeInteger   = "INTEGER"
	dataTypeRecord    = "RECORD"
	dataTypeFloat     = "FLOAT"
	dataTypeBool      = "BOOLEAN"
	dataTypeTimeStamp = "TIMESTAMP"
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

func buildSchemaFields(typ reflect.Type) (result []*gbigquery.TableFieldSchema, err error) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldType := field.Type
		for fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}

		switch fieldType.Kind() {
		case reflect.Bool:
			result = append(result, &gbigquery.TableFieldSchema{
				Name: field.Name,
				Type: dataTypeBool,
			})
		case reflect.Float32:
			fallthrough
		case reflect.Float64:
			result = append(result, &gbigquery.TableFieldSchema{
				Name: field.Name,
				Type: dataTypeFloat,
			})
		case reflect.String:
			result = append(result, &gbigquery.TableFieldSchema{
				Name: field.Name,
				Type: dataTypeString,
			})
		case reflect.Uint:
			fallthrough
		case reflect.Uint8:
			fallthrough
		case reflect.Uint16:
			fallthrough
		case reflect.Uint32:
			fallthrough
		case reflect.Uint64:
			fallthrough
		case reflect.Int:
			fallthrough
		case reflect.Int8:
			fallthrough
		case reflect.Int16:
			fallthrough
		case reflect.Int32:
			fallthrough
		case reflect.Int64:
			result = append(result, &gbigquery.TableFieldSchema{
				Name: field.Name,
				Type: dataTypeInteger,
			})
		case reflect.Struct:
			switch fieldType {
			case byteStringType:
				result = append(result, &gbigquery.TableFieldSchema{
					Name: field.Name,
					Type: dataTypeString,
				})
			case timeType:
				result = append(result, &gbigquery.TableFieldSchema{
					Name: field.Name,
					Type: dataTypeTimeStamp,
				})
			default:
				var fieldFields []*gbigquery.TableFieldSchema
				if fieldFields, err = buildSchemaFields(fieldType); err != nil {
					return
				}
				result = append(result, &gbigquery.TableFieldSchema{
					Name:   field.Name,
					Type:   dataTypeRecord,
					Fields: fieldFields,
				})
			}
		case reflect.Slice:
			// Assume that slices are byte slices and base64 encoded
			result = append(result, &gbigquery.TableFieldSchema{
				Name: field.Name,
				Type: dataTypeString,
			})
		default:
			err = utils.Errorf("Unsupported kind for schema field: %v", field)
			return
		}

	}

	return
}

func (self *BigQuery) createTable(typ reflect.Type, tablesService *gbigquery.TablesService) (err error) {

	var fields []*gbigquery.TableFieldSchema
	if fields, err = buildSchemaFields(typ); err != nil {
		return
	}
	table := &gbigquery.Table{
		TableReference: &gbigquery.TableReference{
			DatasetId: self.datasetId,
			ProjectId: self.projectId,
			TableId:   typ.Name(),
		},
		Schema: &gbigquery.TableSchema{
			Fields: fields,
		},
	}
	if _, err = tablesService.Insert(self.projectId, self.datasetId, table).Do(); err != nil {
		return
	}
	return
}

func (self *BigQuery) patchTable(typ reflect.Type, table *gbigquery.Table) (err error) {
	fmt.Println("Want to patch table for", typ, "and", table)
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
	typ := reflect.TypeOf(i)
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	tablesService := gbigquery.NewTablesService(self.service)
	table, err := tablesService.Get(self.projectId, self.datasetId, typ.Name()).Do()
	if err != nil {
		if gapiErr, ok := err.(*googleapi.Error); ok && gapiErr.Code == 404 {
			return self.createTable(typ, tablesService)
		} else {
			return
		}
	}
	return self.patchTable(typ, table)
}

func (self *BigQuery) InsertTableData(i interface{}) (err error) {
	request := &gbigquery.TableDataInsertAllRequest{
		// Kind: The resource type of the response.
		//Kind string `json:"kind,omitempty"`

		Rows: buildRows(i),
		//Rows  `json:"rows,omitempty"`
	}

	tabledataService := gbigquery.NewTabledataService(self.service)

	typ := reflect.TypeOf(i)
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	tableDataList, err := tabledataService.InsertAll(self.projectId, self.datasetId, typ.Name(), request).Do()
	if err != nil {
		return
	}
	// Unfound rows are ignored :O
	for _, errors := range tableDataList.InsertErrors {
		for _, errorProto := range errors.Errors {
			fmt.Printf("\nerr:%#v\n", errorProto)
		}
	}
	return
}

func buildRows(i interface{}) (result []*gbigquery.TableDataInsertAllRequestRows) {
	val := reflect.ValueOf(i)
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := reflect.TypeOf(i)
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldName, fieldData := formatData(field, val.FieldByName(field.Name))

		result = append(result, &gbigquery.TableDataInsertAllRequestRows{
			// InsertId string `json:"insertId,omitempty"`

			// Json: [Required] A JSON object that contains a row of data. The
			// object's properties and values must match the destination table's
			// schema.
			Json: map[string]gbigquery.JsonValue{fieldName: fieldData}, //map[string]JsonValue `json:"json,omitempty"`
		},
		)
	}

	for _, derp := range result {
		fmt.Printf("\nrows:%#v\n", derp.Json)
	}
	return
}

func formatData(field reflect.StructField, fieldValue reflect.Value) (fieldName string, fieldData interface{}) {
	fieldType := field.Type
	for fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}
	switch fieldType.Kind() {
	case reflect.Bool:
		return field.Name, fieldValue.Bool()

	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		return field.Name, fieldValue.Float()

	case reflect.String:
		return field.Name, fieldValue.String()

	case reflect.Uint:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		return field.Name, fieldValue.Uint()

	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		return field.Name, fieldValue.Int()

	case reflect.Struct:
		return "StringData", "herp"
		/*switch fieldType {
			case byteStringType:
				return field.Name, string(reflect.ValueOf(field).Bytes())

			case timeType:
				return field.Name, time.Now()

			default:
				var fieldFields []*gbigquery.TableFieldSchema
					if fieldFields, err = buildSchemaFields(fieldType); err != nil {
						return
					}
					result = append(result, &gbigquery.TableFieldSchema{
						Name:   field.Name,
						Type:   dataTypeRecord,
						Fields: fieldFields,
					})
			}

		/*case reflect.Slice:
		// Assume that slices are byte slices and base64 encoded
		result = append(result, &gbigquery.TableFieldSchema{
			Name: field.Name,
			Type: dataTypeString,
		})*/
	default:
		return "StringData", "herp"
		//err = utils.Errorf("Unsupported kind for schema field: %v", field)
		return
	}

	return
}
