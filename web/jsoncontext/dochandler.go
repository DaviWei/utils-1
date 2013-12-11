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

func deref(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

type JSONType struct {
	reflect.Type
}

func (self JSONType) ToMap() map[string]interface{} {
	return self.toMap(map[reflect.Type]bool{})
}

func (self JSONType) toMap(seen map[reflect.Type]bool) (result map[string]interface{}) {
	refType := deref(self.Type)
	result = map[string]interface{}{}
	switch refType.Kind() {
	case reflect.Struct:
		result["Type"] = refType.Name()
		if seen[refType] {
			result["DescribedElsewhere"] = true
		}
		seen[refType] = true
		fields := map[string]interface{}{}
		for i := 0; i < refType.NumField(); i++ {
			field := refType.Field(i)
			if field.Anonymous {
				if field.Type.Kind() == reflect.Struct || (field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct) {
					anonMap := JSONType{field.Type}.toMap(seen)
					for k, v := range anonMap["Fields"].(map[string]interface{}) {
						fields[k] = v
					}
				} else {
					fields[field.Name] = fmt.Sprintf("Don't know how to describe %v", field.Type.Name())
				}
			} else {
				jsonToTag := field.Tag.Get("jsonTo")
				jsonTag := field.Tag.Get("json")
				name := field.Name
				if jsonTag != "-" {
					if jsonTag != "" {
						parts := strings.Split(jsonTag, ",")
						name = parts[0]
					}
					if jsonToTag != "" {
						fields[name] = map[string]interface{}{
							"Type": jsonToTag,
						}
					} else {
						if seen[deref(field.Type)] {
							fields[name] = map[string]interface{}{
								"Type": field.Type.Name(),
							}
						} else {
							fields[name] = JSONType{refType.Field(i).Type}.toMap(seen)
						}
					}
				}
			}
		}
		result["Fields"] = fields
	case reflect.Slice:
		if seen[deref(refType.Elem())] {
			result["Elem"] = map[string]interface{}{
				"Type": refType.Elem().Name(),
			}
		} else {
			result["Elem"] = JSONType{refType.Elem()}.toMap(seen)
		}
	default:
		result["Type"] = refType.Name()
	}
	return
}

func (self JSONType) MarshalJSON() (b []byte, err error) {
	return json.Marshal(self.ToMap())
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
		docRoute.In = &JSONType{fType.In(1)}
	}
	if fType.NumOut() == 3 {
		docRoute.Out = &JSONType{fType.Out(1)}
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
