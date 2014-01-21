package jsoncontext

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/gorilla/mux"
	"github.com/soundtrackyourbrand/utils"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
)

var knownEncodings = map[reflect.Type]string{
	reflect.TypeOf(time.Time{}):      "string",
	reflect.TypeOf(time.Duration(0)): "int",
}

var knownDocTags = map[reflect.Type]string{
	reflect.TypeOf(time.Duration(0)): "Duration in nanoseconds",
	reflect.TypeOf(time.Time{}):      "Time encoded like '2013-12-12T20:52:20.963842672+01:00'",
}

var DefaultDocTemplate *template.Template

var DefaultDocTemplateContent = `
<html>
<head>
<link rel="stylesheet" href="//netdna.bootstrapcdn.com/bootstrap/3.0.3/css/bootstrap.min.css">
<link rel="stylesheet" href="//netdna.bootstrapcdn.com/bootstrap/3.0.3/css/bootstrap-theme.min.css">
<script src="http://code.jquery.com/jquery-1.10.1.min.js"></script>
<script src="//netdna.bootstrapcdn.com/bootstrap/3.0.3/js/bootstrap.min.js"></script>
<style type="text/css">
table {
	width: 100%;
}
.spec caption {
	text-align: left;
}
.spec em {
	float: right;
}
</style>
<script>
$(document).ready(function() {
	$('.type-template').on('click', '.tab-switch', function(ev) {
		ev.preventDefault();
		var par = $(ev.target).closest('.type-template');
		par.children('.tab').addClass('hidden');
		par.children('.' + $(ev.target).attr('data-tab')).removeClass('hidden');
		par.children('ul').children('li').removeClass('active');
		par.children('ul').children('li.' + $(ev.target).attr('data-tab')).addClass('active');
	});
});
</script>
</head>
<body>
{{range .Endpoints}}
<div class="panel-group" id="accordion">
{{RenderEndpoint .}}
</div>
{{end}}
</body>
`

var DefaultTypeTemplateContent = `
<div class="type-template">
<ul class="nav nav-tabs">
<li class="active example"><a data-tab="example" class="tab-switch" href="#">Example</a></li>
<li class="spec"><a data-tab="spec" class="tab-switch" href="#">Spec</a></li>
</ul>
<pre class="example tab">
{{Example .Type}}
</pre>
<table class="spec tab table-bordered hidden">
<caption><strong>{{.Type.Type}}</strong>{{if .Type.Comment}}<em>{{.Type.Comment}}</em>{{end}}</caption>
{{if .Type.Scopes}}
<tr><td>Scopes</td><td>{{.Type.Scopes}}</td></tr>
{{end}}
{{if .Type.Elem}}
<tr><td valign="top">Element</td><td>{{RenderSubType .Type.Elem .Stack}}</td></tr>
{{end}}
{{ $stack := .Stack }}
{{range $name, $typ := .Type.Fields}}
<tr><td valign="top">{{$name}}</td><td>{{RenderSubType $typ $stack}}</td></tr>
{{end}}
</table>
</div>
`

var DefaultEndpointTemplateContent = `
<div class="panel panel-default">
  <div class="panel-heading" data-toggle="collapse" href="#collapse-{{UUID}}">
    <h4 class="panel-title">
      <a>
        {{.Methods}} {{.Path}}
      </a>
    </h4>
  </div>
  <div id="collapse-{{UUID}}" class="panel-collapse collapse">
    <div class="panel-body">
      <table class="table-bordered">
			<tr>
  			<td valign="top">curl</td>
				<td>
				<pre>curl{{if .In}} -H "Content-Type: application/json" {{end}}{{if .Scopes}} -H "Authorization: Bearer ${TOKEN}"{{end}} -X{{First .Methods}} ${HOST}{{.Path}}{{if .In}} -d'{{Example .In}}'{{end}}</pre>
				</td>
			</tr>
      {{if .MinAPIVersion}}
        <tr>
          <td>Minimum API version</td>
          <td>{{.MinAPIVersion}}</td>
        </tr>
      {{end}}
      {{if .Scopes}}
        <tr>
          <td>Scopes</td>
          <td>{{.Scopes}}</td>
        </tr>
      {{end}}
			{{if .In}}
			  <tr>
				  <td valign="top">JSON request body</td>
					<td>{{RenderType .In}}</td>
				</tr>
			{{end}}
			{{if .Out}}
			  <tr>
				  <td valign="top">JSON response body</td>
					<td>{{RenderType .Out}}</td>
				</tr>
			{{end}}
      </table>
    </div>
  </div>
</div>
`

func first(i interface{}) string {
	return fmt.Sprint(reflect.ValueOf(i).Index(0).Interface())
}

func init() {
	DefaultDocTemplate = template.Must(template.New("DefaultDocTemplate").Funcs(map[string]interface{}{
		"RenderEndpoint": func(r DocumentedRoute) (result string, err error) {
			return
		},
		"JSON": func(i interface{}) (result string, err error) {
			b, err := json.MarshalIndent(i, "", "  ")
			if err != nil {
				return
			}
			result = string(b)
			return
		},
		"UUID": func() string {
			return ""
		},
		"RenderSubType": func(t JSONType, stack []string) (result string, err error) {
			return
		},
		"RenderType": func(t JSONType) (result string, err error) {
			return
		},
		"Example": func(r JSONType) (result string, err error) {
			return
		},
		"First": first,
	}).Parse(DefaultDocTemplateContent))
	template.Must(DefaultDocTemplate.New("EndpointTemplate").Parse(DefaultEndpointTemplateContent))
	template.Must(DefaultDocTemplate.New("TypeTemplate").Parse(DefaultTypeTemplateContent))
	DefaultDocHandler = DocHandler(DefaultDocTemplate)
}

var DefaultDocHandler http.Handler

type DocumentedRoute interface {
	Render(*template.Template) (string, error)
	GetScopes() []string
	GetSortString() string
}

type DocumentedRoutes []DocumentedRoute

var routes = DocumentedRoutes{}

func (a DocumentedRoutes) Len() int           { return len(a) }
func (a DocumentedRoutes) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DocumentedRoutes) Less(i, j int) bool { return a[i].GetSortString() < a[j].GetSortString() }

type JSONType struct {
	In          bool
	ReflectType reflect.Type
	Type        string
	Fields      map[string]*JSONType
	Scopes      []string
	Elem        *JSONType
	Comment     string
}

func newJSONType(in bool, t reflect.Type, filterOnScopes bool, relevantScopes ...string) (result *JSONType) {
	return newJSONTypeLoopProtector(map[reflect.Type]*JSONType{}, in, t, filterOnScopes, relevantScopes...)
}

func newJSONTypeLoopProtector(seen map[reflect.Type]*JSONType, in bool, t reflect.Type, filterOnScopes bool, relevantScopes ...string) (result *JSONType) {
	if old, found := seen[t]; found {
		result = old
		return
	}
	result = &JSONType{
		In:          in,
		ReflectType: t,
	}
	seen[t] = result
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		result.Type = t.Name()
		result.Fields = map[string]*JSONType{}
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" {
				continue
			}
			if field.Anonymous {
				if field.Type.Kind() == reflect.Struct || (field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct) {
					anonType := newJSONTypeLoopProtector(seen, in, field.Type, filterOnScopes, relevantScopes...)
					for name, typ := range anonType.Fields {
						result.Fields[name] = typ
					}
				} else {
					result.Fields[field.Name] = &JSONType{
						In:          in,
						ReflectType: field.Type,
						Type:        fmt.Sprintf("Don't know how to describe anonymous field that isn't struct or pointer to struct", field.Type.Name()),
					}
				}
			} else {
				jsonToTag := field.Tag.Get("jsonTo")
				jsonTag := field.Tag.Get("json")
				docTag := field.Tag.Get("jsonDoc")
				updateScopesTag := field.Tag.Get("update_scopes")
				name := field.Name
				updateScopes := []string{}
				if jsonTag != "-" {
					if jsonTag != "" {
						parts := strings.Split(jsonTag, ",")
						name = parts[0]
					}
					if updateScopesTag != "" {
						for _, updateScope := range strings.Split(updateScopesTag, ",") {
							for _, relevantScope := range relevantScopes {
								if updateScope == relevantScope {
									updateScopes = append(updateScopes, updateScope)
								}
							}
						}
					}
					if !filterOnScopes || len(updateScopes) > 0 {
						if jsonToTag == "" && knownEncodings[field.Type] != "" {
							jsonToTag = knownEncodings[field.Type]
						}
						if docTag == "" && knownDocTags[field.Type] != "" {
							docTag = knownDocTags[field.Type]
						}
						if jsonToTag != "" {
							result.Fields[name] = &JSONType{
								In:          in,
								ReflectType: field.Type,
								Type:        jsonToTag,
								Comment:     docTag,
							}
						} else {
							result.Fields[name] = newJSONTypeLoopProtector(seen, in, field.Type, filterOnScopes, relevantScopes...)
							result.Fields[name].Comment = docTag
						}
						result.Fields[name].Scopes = updateScopes
					}
				}
			}
		}
	case reflect.Slice:
		result.Type = "Array"
		result.Elem = newJSONTypeLoopProtector(seen, in, t.Elem(), filterOnScopes, relevantScopes...)
	default:
		result.Type = t.Name()
	}
	return
}

type DefaultDocumentedRoute struct {
	Methods       []string
	Path          string
	Scopes        []string
	MinAPIVersion int
	In            *JSONType
	Out           *JSONType
}

func (self *DefaultDocumentedRoute) GetScopes() []string {
	return self.Scopes
}

func (self *DefaultDocumentedRoute) Render(templ *template.Template) (result string, err error) {
	buf := &bytes.Buffer{}
	r := utils.RandomString(10)
	if err = templ.Funcs(map[string]interface{}{
		"UUID": func() string {
			return r
		},
	}).Execute(buf, self); err != nil {
		return
	}
	result = buf.String()
	return
}

func (self *DefaultDocumentedRoute) GetSortString() string {
	return self.Path + self.Methods[0]
}

/*
Remember will record the doc and make sure it shows up in the documentation.
*/
func Remember(doc DocumentedRoute) {
	routes = append(routes, doc)
}

func CreateResponseFunc(fType reflect.Type, fVal reflect.Value) func(c JSONContextLogger) (response Resp, err error) {
	return func(c JSONContextLogger) (response Resp, err error) {
		args := make([]reflect.Value, fType.NumIn())
		args[0] = reflect.ValueOf(c)
		if fType.NumIn() == 2 {
			if fType.In(1).Kind() == reflect.Ptr {
				in := reflect.New(fType.In(1).Elem())
				if err = c.DecodeJSON(in.Interface()); err != nil {
					return
				}
				args[1] = in
			} else {
				in := reflect.New(fType.In(1))
				if err = c.LoadJSON(in.Interface()); err != nil {
					return
				}
				args[1] = in.Elem()
			}
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
}

/*
Document will take a func, a path, a set of methods (separated by |) and a set of scopes that will be used when updating models in the func, and return a documented route and a function suitable for HandlerFunc.

The input func must match func(context JSONContextLogger) (status int, err error)

One extra input argument after context is allowed, and will be JSON decoded from the request body, and used in the documentation struct.

One extra return value between status and error is allowed, and will be JSON encoded to the response body, and used in the documentation struct.
*/
func Document(fIn interface{}, path string, methods string, minAPIVersion int, scopes ...string) (docRoute *DefaultDocumentedRoute, fOut func(JSONContextLogger) (Resp, error)) {
	if errs := utils.ValidateFuncInputs(fIn, []reflect.Type{
		reflect.TypeOf((*JSONContextLogger)(nil)).Elem(),
		reflect.TypeOf((*interface{})(nil)).Elem(),
	}, []reflect.Type{
		reflect.TypeOf((*JSONContextLogger)(nil)).Elem(),
	}); len(errs) == 2 {
		panic(fmt.Errorf("%v does not conform. Fix one of %+v", runtime.FuncForPC(reflect.ValueOf(fIn).Pointer()).Name(), errs))
	}
	if errs := utils.ValidateFuncOutputs(fIn, []reflect.Type{
		reflect.TypeOf(0),
		reflect.TypeOf((*interface{})(nil)).Elem(),
		reflect.TypeOf((*error)(nil)).Elem(),
	}, []reflect.Type{
		reflect.TypeOf(0),
		reflect.TypeOf((*error)(nil)).Elem(),
	}); len(errs) == 2 {
		panic(fmt.Errorf("%v does not conform. Fix one of %+v", runtime.FuncForPC(reflect.ValueOf(fIn).Pointer()).Name(), errs))
	}

	docRoute = &DefaultDocumentedRoute{
		Path:          path,
		Methods:       strings.Split(methods, "|"),
		MinAPIVersion: minAPIVersion,
		Scopes:        scopes,
	}
	fVal := reflect.ValueOf(fIn)
	fType := fVal.Type()
	if fType.NumIn() == 2 {
		docRoute.In = newJSONType(true, fType.In(1), true, scopes...)
	}
	if fType.NumOut() == 3 {
		docRoute.Out = newJSONType(false, fType.Out(1), false)
	}

	fOut = CreateResponseFunc(fType, fVal)
	return
}

func DocHandler(templ *template.Template) http.Handler {
	return httpcontext.HandlerFunc(func(c httpcontext.HTTPContextLogger) (err error) {
		c.Resp().Header().Set("Content-Type", "text/html; charset=UTF-8")
		renderType := func(t JSONType, stack []string) (result string, err error) {
			for _, parent := range stack {
				if parent != "" && parent == t.ReflectType.Name() {
					result = fmt.Sprintf("[loop protector enabled, render stack: %v]", stack)
					return
				}
			}
			stack = append(stack, t.ReflectType.Name())
			buf := &bytes.Buffer{}
			if err = templ.ExecuteTemplate(buf, "TypeTemplate", map[string]interface{}{
				"Type":  t,
				"Stack": stack,
			}); err != nil {
				return
			}
			result = buf.String()
			return
		}

		sort.Sort(routes)
		err = templ.Funcs(map[string]interface{}{
			"RenderEndpoint": func(r DocumentedRoute) (string, error) {
				return r.Render(templ.Lookup("EndpointTemplate"))
			},
			"RenderSubType": func(t JSONType, stack []string) (result string, err error) {
				return renderType(t, stack)
			},
			"RenderType": func(t JSONType) (result string, err error) {
				return renderType(t, nil)
			},
			"First": first,
			"Example": func(r JSONType) (result string, err error) {
				defer func() {
					if e := recover(); e != nil {
						result = fmt.Sprintf("%v\n%s", e, debug.Stack())
					}
				}()
				x := utils.Example(r.ReflectType)
				b, err := json.MarshalIndent(x, "", "  ")
				if err != nil {
					return
				}
				if len(r.Fields) > 0 {
					var i interface{}
					if err = json.Unmarshal(b, &i); err != nil {
						return
					}
					if m, ok := i.(map[string]interface{}); ok {
						newMap := map[string]interface{}{}
						for k, v := range m {
							if _, found := r.Fields[k]; found {
								newMap[k] = v
							}
						}
						if b, err = json.MarshalIndent(newMap, "", "  "); err != nil {
							return
						}
					}
				}
				result = string(b)
				return
			},
		}).Execute(c.Resp(), map[string]interface{}{
			"Endpoints": routes,
		})
		return
	})
}

func DocHandle(router *mux.Router, f interface{}, path string, method string, minAPIVersion int, scopes ...string) {
	doc, fu := Document(f, path, method, minAPIVersion, scopes...)
	Remember(doc)
	methods := strings.Split(method, "|")
	router.Path(path).Methods(methods...).MatcherFunc(MinAPIVersionMatcher(minAPIVersion)).Handler(HandlerFunc(fu, minAPIVersion, scopes...))
}
