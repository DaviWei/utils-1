package jsoncontext

import (
	"encoding/json"
	"fmt"
	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

type JSONContext interface {
	httpcontext.HTTPContext
	APIVersion() int
	DecodeJSON(i interface{}) error
	LoadJSON(i interface{}, accessScope string) error
}

type JSONContextLogger interface {
	JSONContext
	httpcontext.Logger
}

type DefaultJSONContext struct {
	httpcontext.HTTPContextLogger
	apiVersion int
}

func NewJSONContext(c httpcontext.HTTPContextLogger) (result *DefaultJSONContext) {
	result = &DefaultJSONContext{
		HTTPContextLogger: c,
	}
	if result.Req() != nil {
		if header := result.Req().Header.Get("X-API-Version"); header != "" {
			if version, err := strconv.Atoi(header); err == nil {
				result.apiVersion = version
			}
		}
	}
	return
}

func (self *DefaultJSONContext) DecodeJSON(i interface{}) error {
	return json.NewDecoder(self.Req().Body).Decode(i)
}

func (self *DefaultJSONContext) LoadJSON(out interface{}, accessScope string) (err error) {

	var decodedJSON map[string]*json.RawMessage
	if err = json.NewDecoder(self.Req().Body).Decode(&decodedJSON); err != nil {
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

func (self *DefaultJSONContext) APIVersion() int {
	return self.apiVersion
}

type Resp struct {
	Status   int
	Location string
	Body     interface{}
}

func (self Resp) GetStatus() int {
	return self.Status
}

func (self Resp) GetLocation() string {
	return self.Location
}

func (self Resp) Error() string {
	return fmt.Sprint(self.Body)
}

func (self Resp) Write(w http.ResponseWriter) error {
	if self.Body != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		return json.NewEncoder(w).Encode(self.Body)
	}
	return nil
}

type Error struct {
	Resp
	Cause error
	Info  string
}

func NewError(status int, body interface{}, info string, cause error) Error {
	return Error{
		Resp: Resp{
			Status: status,
			Body:   body,
		},
		Cause: cause,
		Info:  info,
	}
}

func HandlerFunc(f func(c JSONContextLogger) (Resp, error)) http.Handler {
	return httpcontext.HandlerFunc(func(cont httpcontext.HTTPContextLogger) (err error) {
		c := NewJSONContext(cont)
		var resp httpcontext.Response
		resp, err = f(c)
		if err == nil {
			c.Render(resp)
		}
		return
	})
}
