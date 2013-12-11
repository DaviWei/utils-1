package jsoncontext

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/soundtrackyourbrand/utils"
	"io"
	"net/http"
	"reflect"
)

type DocumentedRoute interface {
	Write(io.Writer) error
}

var routes = []DocumentedRoute{}

type DocType struct {
	reflect.Type
}

func (self DocType) ToMap() (result map[string]interface{}) {
	refType := reflect.Type(self)
	result = map[string]interface{}{
		"Kind": refType.Kind().String(),
	}
	switch refType.Kind() {
	case reflect.Struct:
		result["Type"] = refType.Name()
		fields := map[string]interface{}{}
		for i := 0; i < refType.NumField(); i++ {
			fields[refType.Field(i).Name] = DocType{refType.Field(i).Type}.ToMap()
		}
		result["Fields"] = fields
	case reflect.Slice:
		result["Elem"] = DocType{refType.Elem()}.ToMap()
	}
	return
}

func (self DocType) MarshalJSON() (b []byte, err error) {
	return json.Marshal(self.ToMap())
}

type DefaultDocumentedRoute struct {
	Methods []string
	Path    string
	In      DocType
	Out     DocType
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
		docRoute.In = DocType{fType.In(1)}
	}
	if fType.NumOut() == 3 {
		docRoute.Out = DocType{fType.Out(1)}
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

var DocHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	for _, route := range routes {
		if err := route.Write(w); err != nil {
			w.WriteHeader(500)
			fmt.Fprintln(w, err)
		}
	}
})

func DocHandle(router *mux.Router, path string, methods string, f interface{}) {
	doc, fu := Document(f, path, methods)
	Remember(doc)
	router.Path(path).Methods(methods).Handler(HandlerFunc(fu))
}
