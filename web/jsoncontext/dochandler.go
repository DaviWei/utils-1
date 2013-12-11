package jsoncontext

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
	"io"
	"reflect"
	"strings"
)

type DocumentedRoute interface {
	Write(io.Writer) error
}

var routes = []DocumentedRoute{}

type JSONType struct {
	Type    string
	Fields  map[string]*JSONType `json:",omitempty"`
	Elem    *JSONType            `json:",omitempty"`
	Comment string               `json:",omitempty"`
}

func newJSONType(t reflect.Type) (result *JSONType) {
	result = &JSONType{}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		result.Type = t.Name()
		result.Fields = map[string]*JSONType{}
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.Anonymous {
				if field.Type.Kind() == reflect.Struct || (field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct) {
					anonType := newJSONType(field.Type)
					for name, typ := range anonType.Fields {
						result.Fields[name] = typ
					}
				} else {
					result.Fields[field.Name] = &JSONType{
						Type: fmt.Sprintf("Don't know how to describe anonymous field that isn't struct or pointer to struct", field.Type.Name()),
					}
				}
			} else {
				jsonToTag := field.Tag.Get("jsonTo")
				jsonTag := field.Tag.Get("json")
				docTag := field.Tag.Get("jsonDoc")
				name := field.Name
				if jsonTag != "-" {
					if jsonTag != "" {
						parts := strings.Split(jsonTag, ",")
						name = parts[0]
					}
					if jsonToTag != "" {
						result.Fields[name] = &JSONType{
							Type:    jsonToTag,
							Comment: docTag,
						}
					} else {
						result.Fields[name] = newJSONType(field.Type)
						result.Fields[name].Comment = docTag
					}
				}
			}
		}
	case reflect.Slice:
		result.Elem = newJSONType(t.Elem())
	default:
		result.Type = t.Name()
	}
	return
}

type DefaultDocumentedRoute struct {
	Methods []string
	Path    string
	In      *JSONType
	Out     *JSONType
}

func (self *DefaultDocumentedRoute) Write(w io.Writer) error {
	return json.NewEncoder(w).Encode(self)
}

/*
Remember will record the doc and make sure it shows up in the documentation.
*/
func Remember(doc DocumentedRoute) {
	routes = append(routes, doc)
}

/*
Document will take a path, a set of methods and a func, and return a documented route and a function suitable for HandlerFunc.

The input func must match func(context JSONContextLogger) (status int, err error)

One extra input argument after context is allowed, and will be JSON decoded from the request body, and used in the documentation struct.

One extra return value between status and error is allowed, and will be JSON encoded to the response body, and used in the documentation struct.
*/
func Document(fIn interface{}, path string, methods ...string) (docRoute *DefaultDocumentedRoute, fOut func(JSONContextLogger) (Resp, error)) {
	if errs := utils.ValidateFuncInputs(fIn, []reflect.Type{
		reflect.TypeOf((*JSONContextLogger)(nil)).Elem(),
		reflect.TypeOf((*interface{})(nil)).Elem(),
	}, []reflect.Type{
		reflect.TypeOf((*JSONContextLogger)(nil)).Elem(),
	}); len(errs) == 2 {
		panic(fmt.Errorf("%v does not conform. Fix one of %v", errs))
	}
	if errs := utils.ValidateFuncOutputs(fIn, []reflect.Type{
		reflect.TypeOf(0),
		reflect.TypeOf((*interface{})(nil)).Elem(),
		reflect.TypeOf((*error)(nil)).Elem(),
	}, []reflect.Type{
		reflect.TypeOf(0),
		reflect.TypeOf((*error)(nil)).Elem(),
	}); len(errs) == 2 {
		panic(fmt.Errorf("%v does not conform. Fix one of %v", errs))
	}

	docRoute = &DefaultDocumentedRoute{
		Path:    path,
		Methods: methods,
	}
	fVal := reflect.ValueOf(fIn)
	fType := fVal.Type()
	if fType.NumIn() == 2 {
		docRoute.In = newJSONType(fType.In(1))
	}
	if fType.NumOut() == 3 {
		docRoute.Out = newJSONType(fType.Out(1))
	}

	fOut = func(c JSONContextLogger) (response Resp, err error) {
		args := make([]reflect.Value, fType.NumIn())
		args[0] = reflect.ValueOf(c)
		if fType.NumIn() == 2 {
			in := reflect.New(fType.In(1))
			if err = c.DecodeJSON(in.Interface()); err != nil {
				return
			}
			args[1] = in
		}
		results := fVal.Call(args)
		if !results[len(results)-1].IsNil() {
			err = results[len(results)-1].Interface().(error)
			return
		}
		if status := int(results[0].Int()); status != 0 {
			response.Status = status
		}
		if len(results) == 3 {
			response.Body = results[1].Interface()
		}
		return
	}
	return
}

var DocHandler = httpcontext.HandlerFunc(func(c httpcontext.HTTPContextLogger) (err error) {
	for _, route := range routes {
		if err = route.Write(c.Resp()); err != nil {
			return
		}
	}
	return
})

func DocHandle(router *mux.Router, path string, methods string, f interface{}) {
	doc, fu := Document(f, path, methods)
	Remember(doc)
	router.Path(path).Methods(methods).Handler(HandlerFunc(fu))
}
