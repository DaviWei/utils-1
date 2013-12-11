package jsoncontext

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	jsonUtils "github.com/soundtrackyourbrand/utils/json"
	"github.com/soundtrackyourbrand/utils/web/httpcontext"
	"net/http"
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
		if header := result.Req().Header.Get(APIVersionHeader); header != "" {
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
	return jsonUtils.LoadJSON(self.Req().Body, out, accessScope)
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

func (self Resp) Write(w http.ResponseWriter) error {
	if self.Body != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	}
	if self.Status != 0 {
		w.WriteHeader(self.Status)
	}
	if self.Body != nil {
		return json.NewEncoder(w).Encode(self.Body)
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

func (self ValidationError) Write(w http.ResponseWriter) error {
	if self.Fields != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	}
	if self.Status != 0 {
		w.WriteHeader(self.Status)
	}
	return json.NewEncoder(w).Encode(self)
	return nil
}

func HandlerFunc(f func(c JSONContextLogger) (Resp, error)) http.Handler {
	return httpcontext.HandlerFunc(func(cont httpcontext.HTTPContextLogger) (err error) {
		c := NewJSONContext(cont)
		resp, err := f(c)
		if err == nil {
			c.Render(resp)
		}
		return
	})
}
