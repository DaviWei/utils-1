package bigquery

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/json"

	gbigquery "code.google.com/p/google-api-go-client/bigquery/v2"
	"code.google.com/p/google-api-go-client/googleapi"
)

var timeType = reflect.TypeOf(time.Now())
var jsonTimeType = reflect.TypeOf(utils.Time{})
var byteStringType = reflect.TypeOf(utils.ByteString{[]byte{0}})

const (
	dataTypeString    = "STRING"
	dataTypeInteger   = "INTEGER"
	dataTypeRecord    = "RECORD"
	dataTypeFloat     = "FLOAT"
	dataTypeBool      = "BOOLEAN"
	dataTypeTimeStamp = "TIMESTAMP"
)

const (
	dataModeRepeated = "REPEATED"
)

type Logger interface {
	Infof(f string, args ...interface{})
}

type BigQuery struct {
	service   *gbigquery.Service
	projectId string
	datasetId string
	logger    Logger
}

func (self *BigQuery) SetLogger(l Logger) {
	self.logger = l
}

func (self *BigQuery) Infof(f string, args ...interface{}) {
	if self.logger != nil {
		self.logger.Infof(f, args...)
	}
}

func (self *BigQuery) GetService() *gbigquery.Service {
	return self.service
}

func (self *BigQuery) GetProjectId() string {
	return self.projectId
}

func (self *BigQuery) GetDatasetId() string {
	return self.datasetId
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

func (self *BigQuery) buildSchemaField(fieldType reflect.Type, name string, seenFieldNames map[string]struct{}) (result *gbigquery.TableFieldSchema, err error) {
	for fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}
	switch fieldType.Kind() {
	case reflect.Bool:
		result = &gbigquery.TableFieldSchema{
			Name: name,
			Type: dataTypeBool,
		}
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		result = &gbigquery.TableFieldSchema{
			Name: name,
			Type: dataTypeFloat,
		}
	case reflect.String:
		result = &gbigquery.TableFieldSchema{
			Name: name,
			Type: dataTypeString,
		}
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
		result = &gbigquery.TableFieldSchema{
			Name: name,
			Type: dataTypeInteger,
		}
	case reflect.Struct:
		switch fieldType {
		case byteStringType:
			result = &gbigquery.TableFieldSchema{
				Name: name,
				Type: dataTypeString,
			}
		case timeType:
			result = &gbigquery.TableFieldSchema{
				Name: name,
				Type: dataTypeTimeStamp,
			}
		case jsonTimeType:
			result = &gbigquery.TableFieldSchema{
				Name: name,
				Type: dataTypeTimeStamp,
			}
		default:
			var fieldFields []*gbigquery.TableFieldSchema
			if fieldFields, err = self.buildSchemaFields(fieldType, seenFieldNames); err != nil {
				return
			}
			result = &gbigquery.TableFieldSchema{
				Name:   name,
				Type:   dataTypeRecord,
				Fields: fieldFields,
			}
		}
	case reflect.Slice:
		switch fieldType {
		case byteStringType:
			result = &gbigquery.TableFieldSchema{
				Name: name,
				Type: dataTypeString,
			}
		default:
			if result, err = self.buildSchemaField(fieldType.Elem(), name, seenFieldNames); err != nil {
				return
			}
			result.Mode = dataModeRepeated
		}
	case reflect.Map:
		self.Infof("Ignoring field %v of type map", name)
		return
	default:
		err = utils.Errorf("Unsupported kind for schema field: %v", fieldType)
		return
	}
	return
}

func (self *BigQuery) buildSchemaFields(typ reflect.Type, seenFieldNames map[string]struct{}) (result []*gbigquery.TableFieldSchema, err error) {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldType := field.Type
		for fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		name := field.Name
		if jsonTag := field.Tag.Get("json"); jsonTag != "" {
			if splitTag := strings.Split(jsonTag, ","); splitTag[0] != "" {
				name = splitTag[0]
			}
		}
		if name == "-" {
			continue
		}
		if bigqueryTag := field.Tag.Get("bigquery"); bigqueryTag == "-" {
			continue
		}
		if _, found := seenFieldNames[name]; found {
			continue
		}
		seenFieldNames[name] = struct{}{}

		var thisField *gbigquery.TableFieldSchema
		seenFieldNamesToSend := seenFieldNames
		if !field.Anonymous {
			seenFieldNamesToSend = map[string]struct{}{}
		}
		if thisField, err = self.buildSchemaField(fieldType, name, seenFieldNamesToSend); err != nil {
			return
		}
		if thisField != nil {
			if field.Anonymous {
				result = append(result, thisField.Fields...)
			} else {
				result = append(result, thisField)
			}
		}
	}

	return
}

func (self *BigQuery) buildTable(typ reflect.Type) (result *gbigquery.Table, err error) {
	var fields []*gbigquery.TableFieldSchema
	if fields, err = self.buildSchemaFields(typ, map[string]struct{}{}); err != nil {
		return
	}
	fields = append(fields, &gbigquery.TableFieldSchema{
		Name: "_inserted_at",
		Type: dataTypeTimeStamp,
	})
	result = &gbigquery.Table{
		TableReference: &gbigquery.TableReference{
			DatasetId: self.datasetId,
			ProjectId: self.projectId,
			TableId:   typ.Name(),
		},
		Schema: &gbigquery.TableSchema{
			Fields: fields,
		},
	}
	return
}

func (self *BigQuery) createTable(typ reflect.Type, tablesService *gbigquery.TablesService) (err error) {
	table, err := self.buildTable(typ)
	if err != nil {
		return
	}
	if _, err = tablesService.Insert(self.projectId, self.datasetId, table).Do(); err != nil {
		if gapiErr, ok := err.(*googleapi.Error); ok && gapiErr.Code == 409 {
			self.Infof("Unable to create table for %v, someone else already did it", typ)
			err = nil
			return
		}
		err = utils.Errorf("Unable to create %#v with\n%v\n%v", typ.Name(), utils.Prettify(table), err)
		return
	}
	return
}

func (self *BigQuery) patchTable(typ reflect.Type, tablesService *gbigquery.TablesService, originalTable *gbigquery.Table) (err error) {

	table, err := self.buildTable(typ)
	if err != nil {
		return
	}

	unionTable := self.unionTables(table, originalTable)
	if _, err = tablesService.Patch(self.projectId, self.datasetId, originalTable.TableReference.TableId, unionTable).Do(); err != nil {
		err = utils.Errorf("Error trying to patch %#v with\n%v\n%v", typ.Name(), utils.Prettify(unionTable), err)
		return
	}
	return
}

func (self *BigQuery) unionFields(fields1, fields2 []*gbigquery.TableFieldSchema) (result []*gbigquery.TableFieldSchema) {
	unionFields := make(map[string]*gbigquery.TableFieldSchema)

	for _, field := range fields2 {
		unionFields[field.Name] = field
	}
	for index, field := range fields1 {
		if len(field.Fields) == 0 {
			unionFields[field.Name] = field
		} else {
			// Union the nested fields
			unionFields[field.Name] = field
			field.Fields = self.unionFields(fields1[index].Fields, fields1[index].Fields)
		}
	}
	for _, field := range unionFields {
		result = append(result, field)
	}
	return
}

/*
Makes a union of all the columns of given tables.
If a field is present in both tables, table1's field is taken
*/
func (self *BigQuery) unionTables(table1, table2 *gbigquery.Table) (result *gbigquery.Table) {
	var resultFields []*gbigquery.TableFieldSchema
	for _, field := range self.unionFields(table1.Schema.Fields, table2.Schema.Fields) {
		resultFields = append(resultFields, field)
	}

	result = &gbigquery.Table{
		TableReference: &gbigquery.TableReference{
			DatasetId: self.datasetId,
			ProjectId: self.projectId,
			TableId:   table1.TableReference.TableId,
		},
		Schema: &gbigquery.TableSchema{
			Fields: resultFields,
		},
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
	return self.patchTable(typ, tablesService, table)
}

const (
	maxString = 1 << 10
)

func cropStrings(m map[string]gbigquery.JsonValue) {
	for k, v := range m {
		if s, ok := v.(string); ok {
			if len(s) > maxString {
				m[k] = s[:maxString]
			}
		} else if inner, ok := v.(map[string]gbigquery.JsonValue); ok {
			cropStrings(inner)
		}
	}
}

func (self *BigQuery) InsertTableData(i interface{}) (err error) {
	j := map[string]gbigquery.JsonValue{}

	b, err := json.Marshal(i, "bigquery")
	if err != nil {
		return
	}
	if err = json.Unmarshal(b, &j); err != nil {
		return
	}
	cropStrings(j)
	if b, err = time.Now().MarshalJSON(); err != nil {
		return
	}
	s := ""
	if err = json.Unmarshal(b, &s); err != nil {
		return
	}
	j["_inserted_at"] = s

	request := &gbigquery.TableDataInsertAllRequest{
		Rows: []*gbigquery.TableDataInsertAllRequestRows{
			&gbigquery.TableDataInsertAllRequestRows{
				Json: j,
			},
		},
	}

	typ := reflect.TypeOf(i)
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).Tag.Get("bigquery") == "-" {
			name := typ.Field(i).Name
			if jsonTag := typ.Field(i).Tag.Get("json"); jsonTag != "" {
				if splitTag := strings.Split(jsonTag, ","); splitTag[0] != "" {
					name = splitTag[0]
				}
			}
			delete(j, name)
		}
	}

	tabledataService := gbigquery.NewTabledataService(self.GetService())
	tableDataList, err := tabledataService.InsertAll(self.GetProjectId(), self.GetDatasetId(), typ.Name(), request).Do()
	if err != nil {
		return
	}

	// Build insert errors error message
	if len(tableDataList.InsertErrors) != 0 {
		prettyJ := utils.Prettify(j)
		errorStrings := []string{}
		for _, errors := range tableDataList.InsertErrors {
			for _, errorProto := range errors.Errors {
				errorStrings = append(errorStrings, fmt.Sprintf("\nReason:%v,\nMessage:%v,\nLocation:%v", errorProto.Reason, errorProto.Message, errorProto.Location))
			}
		}
		errorStrings = append(errorStrings, fmt.Sprintf("BigQuery: Error inserting json %v into table %v:", prettyJ, typ.Name()))
		err = utils.Errorf(strings.Join(errorStrings, "\n"))
	}

	return
}

/*
Create view of a table defined by a query.
*/
func (self *BigQuery) AssertView(viewName string, query string) (err error) {
	tablesService := gbigquery.NewTablesService(self.service)
	_, err = tablesService.Get(self.projectId, self.datasetId, viewName).Do()
	if err != nil {
		if gapiErr, ok := err.(*googleapi.Error); ok && gapiErr.Code == 404 {
			viewTable := &gbigquery.Table{
				TableReference: &gbigquery.TableReference{
					DatasetId: self.datasetId,
					ProjectId: self.projectId,
					TableId:   viewName,
				},
				View: &gbigquery.ViewDefinition{
					Query: query,
				},
			}
			if _, err = tablesService.Insert(self.projectId, self.datasetId, viewTable).Do(); err != nil {
				if gapiErr, ok := err.(*googleapi.Error); ok && gapiErr.Code == 409 {
					self.Infof("Unable to create %v, someone else already did it", viewName)
					err = nil
					return
				} else {
					err = utils.Errorf("Unable to create %#v with\n%v\n%v", viewName, utils.Prettify(viewTable), err)
					return
				}
			}
		}
	}
	return
}

func (self *BigQuery) addFieldNames(fields []*gbigquery.TableFieldSchema, prefix string, dst *[]string) {
	for _, field := range fields {
		if field.Type == dataTypeRecord {
			self.addFieldNames(field.Fields, prefix+field.Name+".", dst)
		} else {
			*dst = append(*dst, fmt.Sprintf("%v%v AS %v%v", prefix, field.Name, prefix, field.Name))
		}
	}
}

func (self *BigQuery) AssertCurrentVersionView(tableName string) (err error) {
	latestVersionTableName := fmt.Sprintf("LatestVersionOf%v", tableName)
	if err = self.DropTable(latestVersionTableName); err != nil {
		return
	}
	versionTableQuery := fmt.Sprintf("SELECT id, MAX(iso8601_updated_at) AS iso8601_updated_at, FIRST(_inserted_at) AS _inserted_at FROM [warehouse.%v] GROUP BY id", tableName)
	if err = self.AssertView(latestVersionTableName, versionTableQuery); err != nil {
		return
	}

	tablesService := gbigquery.NewTablesService(self.service)
	table, err := tablesService.Get(self.projectId, self.datasetId, tableName).Do()
	if err != nil {
		return
	}
	cols := []string{}
	self.addFieldNames(table.Schema.Fields, "data.", &cols)

	currentTableQuery := fmt.Sprintf("SELECT %v FROM [warehouse.LatestVersionOf%v] AS key "+
		"INNER JOIN [warehouse.%v] AS data ON "+
		"key.id = data.id AND "+
		"key._inserted_at = data._inserted_at AND "+
		"key.iso8601_updated_at = data.iso8601_updated_at", strings.Join(cols, ","), tableName, tableName)

	currentTableName := fmt.Sprintf("Current%v", tableName)
	if err = self.DropTable(currentTableName); err != nil {
		return
	}
	if err = self.AssertView(currentTableName, currentTableQuery); err != nil {
		return
	}
	return
}

func (self *BigQuery) DropTable(tableName string) (err error) {
	tablesService := gbigquery.NewTablesService(self.service)
	if err = tablesService.Delete(self.projectId, self.datasetId, tableName).Do(); err != nil {
		return
	}
	return
}
