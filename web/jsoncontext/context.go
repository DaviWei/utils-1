package jsoncontext

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/soundtrackyourbrand/utils"
	jsonUtils "github.com/soundtrackyourbrand/utils/json"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
	"net/http"
	"reflect"
	"strconv"
)

const (
	APIVersionHeader = "X-API-Version"
)

func MinAPIVersionMatcher(minApiVersion int) mux.MatcherFunc {
	return func(req *http.Request, match *mux.RouteMatch) bool {
		header := req.Header.Get(APIVersionHeader)
		if header == "" {
			return false
		}
		apiVersion, err := strconv.Atoi(header)
		if err != nil {
			return false
		}
		if apiVersion < minApiVersion {
			return false
		}
		return true
	}
}

type JSONContext interface {
	httpcontext.HTTPContext
	APIVersion() int
	DecodeJSON(i interface{}) error
	LoadJSON(i interface{}) error
	CopyJSON(in, out interface{}) error
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
		if header := result.Req().Header.Get(APIVersionHeader); header != "" {
			if version, err := strconv.Atoi(header); err == nil {
				result.apiVersion = version
			}
		}
	}
	return
}

func (self *DefaultJSONContext) CopyJSON(in, out interface{}) (err error) {
	token, err := self.AccessToken(nil)
	if err != nil {
		return
	}
	return jsonUtils.CopyJSON(in, out, token.Scopes()...)
}

func (self *DefaultJSONContext) DecodeJSON(i interface{}) error {
	return json.NewDecoder(self.Req().Body).Decode(i)
}

func (self *DefaultJSONContext) LoadJSON(out interface{}) (err error) {
	at, err := self.AccessToken(nil)
	if err != nil {
		return
	}
	scopes := at.Scopes()
	return jsonUtils.LoadJSON(self.Req().Body, out, scopes...)
}

func (self *DefaultJSONContext) APIVersion() int {
	return self.apiVersion
}

type Resp struct {
	Status int
	Body   interface{}
}

func (self Resp) Error() string {
	return fmt.Sprint(self.Body)
}

type BeforeMarshaller interface {
	BeforeMarshal(c interface{}) error
}

func (self Resp) RunBodyBeforeMarshal(c interface{}) (err error) {
	var runRecursive func(reflect.Value, reflect.Value) error

	cVal := reflect.ValueOf(c)
	contextType := reflect.TypeOf((*JSONContextLogger)(nil)).Elem()
	stackType := reflect.TypeOf([]interface{}{})

	// Validate that c implements JSONContextLogger
	if !cVal.Type().AssignableTo(contextType) {
		return fmt.Errorf("Invalid context type")
	}

	runRecursive = func(val reflect.Value, stack reflect.Value) error {
		stack = reflect.Append(stack, val)

		// Try run BeforeMarshal
		fun := val.MethodByName("BeforeMarshal")
		if fun.IsValid() {

			// Validate BeforeMarshal takes something that implements JSONContextLogger
			if err = utils.ValidateFuncInput(fun.Interface(), []reflect.Type{contextType, stackType}); err != nil {
				return fmt.Errorf("BeforeMarshal needs to take an JSONContextLogger")
			}

			// Validate BeforeMarshal returns an error
			if err = utils.ValidateFuncOutput(fun.Interface(), []reflect.Type{reflect.TypeOf((*error)(nil)).Elem()}); err != nil {
				return fmt.Errorf("BeforeMarshal needs to return an error")
			}

			res := fun.Call([]reflect.Value{cVal, stack})
			if res[0].IsNil() {
				return nil
			} else {
				return res[0].Interface().(error)
			}
		}

		// Try do recursion on these types.
		switch val.Kind() {
		case reflect.Ptr:
			if val.IsNil() {
				return nil
			}
			return runRecursive(val.Elem(), stack)
			break

		case reflect.Slice:
			for i := 0; i < val.Len(); i++ {
				if err := runRecursive(val.Index(i), stack); err != nil {
					return err
				}
			}
			break

		case reflect.Struct:
			for i := 0; i < val.NumField(); i++ {
				if err := runRecursive(val.Field(i), stack); err != nil {
					return err
				}
			}
			break
		}
		return nil
	}

	// Run recursive reflection on self.Body that executes BeforeMarshal on every object possible.
	stack := []interface{}{}
	return runRecursive(reflect.ValueOf(self.Body), reflect.ValueOf(stack))
}

func (self Resp) Respond(c httpcontext.HTTPContextLogger) (err error) {
	if self.Body != nil {
		c.Resp().Header().Set("Content-Type", "application/json; charset=UTF-8")
	}
	if self.Status != 0 {
		c.Resp().WriteHeader(self.Status)
	}
	if self.Body != nil {
		if err = self.RunBodyBeforeMarshal(c); err != nil {
			return
		}
		return json.NewEncoder(c.Resp()).Encode(self.Body)
	}
	return nil
}

type Error struct {
	Resp
	Cause error
	Info  string
}

func (self Error) Error() string {
	return fmt.Sprintf("%+v, %v, %#v", self.Resp, self.Cause, self.Info)
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

type field struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
	Cause   error  `json:"-"`
}

type ValidationError struct {
	Status int
	Cause  error
	Info   string
	Fields map[string]field `json:"fields,omitempty"`
}

func (self *ValidationError) AddField(fieldName, message string, code int, cause error, status int) *ValidationError {
	if self == nil {
		return &ValidationError{
			Fields: map[string]field{
				fieldName: {
					Message: message,
					Code:    code,
					Cause:   cause,
				},
			},
			Status: status,
		}
	}
	if self.Fields == nil {
		self.Fields = make(map[string]field)
	}
	self.Fields[fieldName] = field{
		Message: message,
		Code:    code,
		Cause:   cause,
	}
	if status > self.Status {
		self.Status = status
	}
	return self
}

func (self ValidationError) Error() string {
	return fmt.Sprint(self.Fields)
}

func (self ValidationError) Respond(c httpcontext.HTTPContextLogger) error {
	if self.Fields != nil {
		c.Resp().Header().Set("Content-Type", "application/json; charset=UTF-8")
	}
	if self.Status != 0 {
		c.Resp().WriteHeader(self.Status)
	}
	return json.NewEncoder(c.Resp()).Encode(self)
	return nil
}

func Handle(c JSONContextLogger, f func() (Resp, error), minAPIVersion int, scopes ...string) {
	httpcontext.Handle(c, func() (err error) {
		if c.APIVersion() < minAPIVersion {
			err = NewError(417, fmt.Sprintf("X-API-Version header has to request API version greater than 0"), fmt.Sprintf("Headers: %+v", c.Req().Header), nil)
			return
		}
		resp, err := f()
		if err == nil {
			err = resp.Respond(c)
		}
		return
	}, scopes...)
}

func HandlerFunc(f func(c JSONContextLogger) (Resp, error), minAPIVersion int, scopes ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := NewJSONContext(httpcontext.NewHTTPContext(w, r))
		Handle(c, func() (Resp, error) {
			return f(c)
		}, minAPIVersion, scopes...)
	})
}
