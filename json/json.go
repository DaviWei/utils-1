package json

import (
	"encoding/json"
	"fmt"
	"github.com/soundtrackyourbrand/utils"
	"io"
	"reflect"
	"strings"
)

func LoadJSON(in io.Reader, out interface{}, accessScope string) (err error) {

	var decodedJSON map[string]*json.RawMessage
	if err = json.NewDecoder(in).Decode(&decodedJSON); err != nil {
		return
	}

	structPointerValue := reflect.ValueOf(out)
	if structPointerValue.Kind() != reflect.Ptr {
		err = fmt.Errorf("%#v is not a pointer to a struct", out)
		return
	}
	structValue := structPointerValue.Elem()
	if structValue.Kind() != reflect.Struct {
		err = fmt.Errorf("%#v is not a pointer to a struct.", out)
		return
	}

	structType := structValue.Type()
	for i := 0; i < structValue.NumField(); i++ {
		valueField := structValue.Field(i)
		typeField := structType.Field(i)

		allowedScopes := strings.Split(typeField.Tag.Get("update_scopes"), ",")
		jsonAttributeName := typeField.Name
		if jsonTag := typeField.Tag.Get("json"); jsonTag != "" {
			jsonAttributeName = strings.Split(jsonTag, ",")[0]
		}

		// Newer try to update field '-'
		if jsonAttributeName == "-" {
			continue
		}

		// Check if a update for this field exist in the source json data.
		data, found := decodedJSON[jsonAttributeName]
		if !found {
			continue
		}

		// Check that the scope user are in, is allowed to update this field.
		if !utils.InSlice(allowedScopes, accessScope) {
			continue
		}

		// Use json unmarshal the raw value in to correct field.
		if err = json.Unmarshal(*data, valueField.Addr().Interface()); err != nil {
			return
		}
	}
	return
}
