package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

func CopyJSON(in interface{}, out interface{}, accessScopes ...string) (err error) {
	buf := &bytes.Buffer{}
	if err = json.NewEncoder(buf).Encode(in); err != nil {
		return
	}
	err = LoadJSON(buf, out, accessScopes...)
	return
}

/*
LoadJSON will JSON decode in into out, but only the fields of out that have a tag 'update_scopes' matching the provided accessScopes or '*'.
*/
func LoadJSON(in io.Reader, out interface{}, accessScopes ...string) (err error) {

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

		updateScopesTag := typeField.Tag.Get("update_scopes")
		allowedScopes := strings.Split(updateScopesTag, ",")
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

		// Check that at least one of the scopes is allowed to update this field.
		inScope := updateScopesTag == "*"
		if !inScope {
			for _, scope := range accessScopes {
				for _, allowedScope := range allowedScopes {
					if scope == allowedScope {
						inScope = true
						break
					}
				}
			}
		}
		if !inScope {
			continue
		}

		// Use json unmarshal the raw value in to correct field.
		if err = json.Unmarshal(*data, valueField.Addr().Interface()); err != nil {
			return
		}
	}
	return
}
