package jsoncontext

import (
	"encoding/json"
	"fmt"
	"github.com/soundtrackyourbrand/webutils/httpcontext"
	"net/http"
	"strconv"
)

type JSONContext interface {
	httpcontext.HTTPContext
	APIVersion() int
	DecodeJSON(i interface{}) error
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

func (self DefaultJSONContext) DecodeJSON(i interface{}) error {
	return json.NewDecoder(self.Req().Body).Decode(i)
}

func (self DefaultJSONContext) Render(resp Response) error {
	if resp.GetStatus() != 0 {
		self.Resp().WriteHeader(resp.GetStatus())
	}
	return resp.Write(self.Resp())
}

func (self DefaultJSONContext) APIVersion() int {
	return self.apiVersion
}

type Response interface {
	Write(w http.ResponseWriter) error
	GetStatus() int
}

type Resp struct {
	Status int
	Body   interface{}
}

func (self Resp) GetStatus() int {
	return self.Status
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
	return httpcontext.HandlerFunc(func(cont httpcontext.HTTPContextLogger) {
		c := NewJSONContext(cont)
		resp, err := f(c)
		if err != nil {
			if errResponse, ok := err.(Response); ok {
				c.Render(errResponse)
			} else {
				c.Resp().WriteHeader(500)
				fmt.Fprintf(c.Resp(), "%v", err)
			}
			c.Errorf("%+v", err)
			return
		}
		c.Render(resp)
	})
}
